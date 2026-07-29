from __future__ import annotations

import builtins
import copy
import ast

import json
import hashlib
import os
import stat
import socket
import subprocess
from pathlib import Path

import pytest

import herdres
import herdres_gateway
from herdres_connector import ingress_requests, source_sync, state
from herdres_connector.managed_bots import managed_bot_kind_for_key, managed_bot_tokens
from herdres_connector.rendering import render_status_overview
from herdres_connector.rich_delivery import MAX_RICH_HTML_CHARS, render_feed_item_delivery_html_parts, render_turn_item_html, turn_item_from_source
from herdres_connector.safe import public_prune
from herdres_connector.source_sync import SyncRuntime, sync_once
from herdres_connector.telegram_delivery import TelegramClient, drain_outbox
from herdres_connector.ingress_identity import derive_telegram_request_id


REQUEST_ID_KEY = bytes(range(32))
REQUEST_ID = derive_telegram_request_id(
    REQUEST_ID_KEY,
    receiver_id="manager",
    update_id=100,
    chat_id=-100,
    message_id=9001,
)
REQUEST_ID_2 = derive_telegram_request_id(
    REQUEST_ID_KEY,
    receiver_id="manager",
    update_id=101,
    chat_id=-100,
    message_id=9002,
)


def _offlock_protocol_violations(
    source_text: str,
    delivery_text: str,
    decisions_text: str | None = None,
    tendwire_text: str | None = None,
    rich_text: str | None = None,
) -> list[tuple]:
    """Return structural executor escapes in every capability consumer."""

    source_path = Path(source_sync.__file__)
    decisions_text = (
        source_path.with_name("decisions.py").read_text(encoding="utf-8")
        if decisions_text is None
        else decisions_text
    )
    tendwire_text = (
        source_path.with_name("tendwire_client.py").read_text(
            encoding="utf-8"
        )
        if tendwire_text is None
        else tendwire_text
    )
    rich_text = (
        source_path.with_name("rich_delivery.py").read_text(
            encoding="utf-8"
        )
        if rich_text is None
        else rich_text
    )
    read_only_by_provider = source_sync._READ_ONLY_PROVIDER_METHODS
    read_only = set().union(*read_only_by_provider.values())
    capabilities = set(source_sync._DIRECT_PROVIDER_CAPABILITIES)
    capabilities.update(source_sync._ADAPTER_PROVIDER_CAPABILITIES)
    capability_methods = {
        capability.split(".", 1)[1] for capability in capabilities
    }
    capability_methods.update(
        {
        "delete_turn_delivery_message",
        "edit_feed_item",
        "edit_turn_delivery_part",
        "send_feed_item",
        "send_turn_delivery_part",
        }
    )
    violations: list[tuple] = []
    reasons: list[str] = []

    def call_name(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return ""

    def root_name(node):
        current = node
        while isinstance(current, (ast.Attribute, ast.Subscript)):
            current = current.value
        return current.id if isinstance(current, ast.Name) else ""

    def provider_receiver(node):
        if isinstance(node, ast.Name):
            return node.id in {"provider", "guarded"}
        if isinstance(node, ast.Attribute):
            return (
                node.attr in {"telegram", "tendwire"}
                or provider_receiver(node.value)
            )
        return False

    def literal_keyword(node, name):
        value = next(
            (
                keyword.value
                for keyword in node.keywords
                if keyword.arg == name
            ),
            None,
        )
        return value.value if isinstance(value, ast.Constant) else None

    source_tree = ast.parse(source_text)
    parents = {
        child: parent
        for parent in ast.walk(source_tree)
        for child in ast.iter_child_nodes(parent)
    }
    def source_function_name(node):
        current = node
        while current in parents:
            current = parents[current]
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return current.name
        return ""

    unsafe_aliases: dict[str, tuple[int, str]] = {}
    for node in ast.walk(source_tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        targets = (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        unsafe_name = ""
        if (
            isinstance(value, ast.Attribute)
            and provider_receiver(value.value)
            and value.attr not in read_only
        ):
            unsafe_name = value.attr
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "getattr"
            and len(value.args) >= 2
            and isinstance(value.args[1], ast.Constant)
            and provider_receiver(value.args[0])
            and value.args[1].value not in read_only
        ):
            unsafe_name = str(value.args[1].value)
        if not unsafe_name:
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                unsafe_aliases[target.id] = (
                    node.lineno,
                    unsafe_name,
                )

    removed_executor_names = {
        "_invoke_offlock",
        "_invoke_provider_mutation",
        "_offlock_client_internals",
        "_OFFLOCK_EXECUTOR_TOKEN",
        "_require_executor_token",
    }
    executor_allowed = {
        "describe": {"__getattr__"},
        "store": {"__getattr__"},
        "provider_kind": {"__getattr__"},
        "with_token": {"__getattr__"},
        "read": {"__getattr__", "call", "checked_api"},
        "mutation": {
            "__getattr__",
            "_execute_entry_operation",
            "_execute_exact_provider_operation",
        },
        "direct": {
            "_execute_entry_operation",
            "_execute_exact_provider_operation",
        },
    }
    executor_aliases: set[str] = set()
    executor_object_aliases: set[str] = set()
    sensitive_aliases: set[str] = set()
    sensitive_names = removed_executor_names | {
        "_execute_entry_operation",
        "_execute_exact_provider_operation",
        "_exact_provider_client",
        "_provider_mutation",
    }
    alias_changed = True
    while alias_changed:
        pass_changed = False
        for node in ast.walk(source_tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
            )
            target_names = {
                target.id
                for target in targets
                if isinstance(target, ast.Name)
            }
            before = (
                len(executor_aliases),
                len(executor_object_aliases),
                len(sensitive_aliases),
            )
            if (
                isinstance(value, ast.Name)
                and value.id == "_OFFLOCK_EXECUTOR"
            ):
                executor_object_aliases.update(target_names)
            if (
                isinstance(value, ast.Attribute)
                and (
                    (
                        isinstance(value.value, ast.Name)
                        and value.value.id == "_OFFLOCK_EXECUTOR"
                    )
                    or (
                        isinstance(value.value, ast.Name)
                        and value.value.id in executor_object_aliases
                    )
                )
            ):
                executor_aliases.update(target_names)
            if (
                isinstance(value, ast.Name)
                and (
                    value.id in sensitive_names
                    or value.id in sensitive_aliases
                    or value.id == "getattr"
                )
            ):
                sensitive_aliases.update(target_names)
            if (
                isinstance(value, ast.Attribute)
                and value.attr in {
                    "__getattribute__",
                    "_execute_entry",
                    "_execute_exact",
                }
            ):
                sensitive_aliases.update(target_names)
            if before != (
                len(executor_aliases),
                len(executor_object_aliases),
                len(sensitive_aliases),
            ):
                pass_changed = True
        alias_changed = pass_changed

    for node in ast.walk(source_tree):
        owner = source_function_name(node)
        if isinstance(node, ast.Name) and node.id in removed_executor_names:
            violations.append(
                ("removed_executor_escape", node.lineno, owner, node.id)
            )
        if isinstance(node, ast.Attribute) and (
            node.attr == "_client"
            or node.attr.startswith("_OfflockClient__provider")
        ):
            if owner != "internals":
                violations.append(
                    ("raw_client_attribute", node.lineno, owner, node.attr)
                )
        if not isinstance(node, ast.Call):
            continue
        name = call_name(node.func)
        if name in removed_executor_names | {
            "_execute_entry",
            "_execute_exact",
        }:
            violations.append(
                ("internal_executor_escape", node.lineno, owner, name)
            )
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in executor_aliases
        ):
            violations.append(
                ("executor_alias_escape", node.lineno, owner, node.func.id)
            )
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in sensitive_aliases
        ):
            violations.append(
                (
                    "sensitive_alias_escape",
                    node.lineno,
                    owner,
                    node.func.id,
                )
            )
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in executor_object_aliases
        ):
            violations.append(
                ("executor_object_alias_escape", node.lineno, owner, name)
            )
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "_OFFLOCK_EXECUTOR"
            and owner not in executor_allowed.get(name, set())
        ):
            violations.append(
                ("internal_executor_escape", node.lineno, owner, name)
            )
        if (
            name == "__getattribute__"
            and len(node.args) >= 2
            and (
                provider_receiver(node.args[0])
                or (
                    isinstance(node.args[1], ast.Constant)
                    and str(node.args[1].value).startswith(
                        "_OfflockClient__provider"
                    )
                )
            )
            and (
                not isinstance(node.args[1], ast.Constant)
                or str(node.args[1].value).startswith(
                    "_OfflockClient__provider"
                )
            )
            and owner != "internals"
        ):
            violations.append(
                ("reflected_raw_provider", node.lineno, owner)
            )
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in unsafe_aliases
            and owner != "invoke_provider_mutation"
        ):
            violations.append(
                (
                    "raw_mutation_alias",
                    node.lineno,
                    owner,
                    node.func.id,
                    unsafe_aliases[node.func.id],
                )
            )
        if name == "_execute_entry_operation":
            declared = (
                len(node.args) == 4
                and (
                    (
                        isinstance(node.args[3], ast.Call)
                        and isinstance(node.args[3].func, ast.Name)
                        and node.args[3].func.id == "_provider_mutation"
                    )
                    or (
                        owner == "_execute_decision_provider_operation"
                        and isinstance(node.args[3], ast.Name)
                        and node.args[3].id == "mutation"
                    )
                )
            )
            if not declared:
                violations.append(("entry_without_capability", node.lineno))
        if name == "_execute_exact_provider_operation":
            mutation_value = next(
                (
                    keyword.value
                    for keyword in node.keywords
                    if keyword.arg == "mutation"
                ),
                None,
            )
            declared = (
                isinstance(mutation_value, ast.Call)
                and isinstance(mutation_value.func, ast.Name)
                and mutation_value.func.id == "_provider_mutation"
            ) or (
                owner == "_execute_decision_provider_operation"
                and isinstance(mutation_value, ast.Name)
                and mutation_value.id == "mutation"
            )
            if not declared:
                violations.append(("exact_without_capability", node.lineno))
        if name == "_provider_mutation":
            capability = (
                node.args[0].value
                if node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                else None
            )
            reason = literal_keyword(node, "reason")
            if capability is not None:
                if not reason or capability not in reason:
                    violations.append(
                        ("reason_does_not_name_capability", node.lineno)
                    )
                else:
                    reasons.append(reason)
            elif owner not in {
                "_execute_topic_cleanup_targets",
                "_execute_decision_provider_operation",
                "__getattr__",
            }:
                violations.append(("dynamic_capability", node.lineno))
        if name == "_exact_provider_client":
            reason = literal_keyword(node, "reason")
            if not reason or not any(
                provider in reason for provider in ("Telegram", "Tendwire")
            ):
                violations.append(("exact_client_reason", node.lineno))
            else:
                reasons.append(reason)
        if isinstance(node.func, ast.Attribute):
            if (
                provider_receiver(node.func.value)
                and name not in read_only
                and owner != "invoke_provider_mutation"
            ):
                violations.append(
                    ("raw_mutation", node.lineno, owner, name)
                )
        if (
            name == "getattr"
            and len(node.args) >= 2
            and provider_receiver(node.args[0])
            and owner not in {
                "__getattr__",
                "describe",
                "read",
                "invoke_provider_mutation",
            }
        ):
            violations.append(
                (
                    (
                        "computed_provider_getattr"
                        if not isinstance(node.args[1], ast.Constant)
                        else "raw_getattr_mutation"
                    ),
                    node.lineno,
                    owner,
                    (
                        node.args[1].value
                        if isinstance(node.args[1], ast.Constant)
                        else "<computed>"
                    ),
                )
            )

    dispatcher = next(
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == "invoke_provider_mutation"
    )
    for node in ast.walk(dispatcher):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "provider"
            and node.func.attr not in {"send_voice", "with_token"}
        ):
            violations.append(
                ("dispatcher_extra_mutation", node.lineno, node.func.attr)
            )
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Store)
            and isinstance(node.value, ast.Name)
            and node.value.id in {"entry", "store"}
        ):
            violations.append(("dispatcher_state_write", node.lineno))

    # telegram_delivery consumes exact-ID capability proxies. Every mutation,
    # including lease acquisition, is enumerated with a unique written reason.
    exact_reasons = {
        ("connector_poll", 0): (
            "tendwire.connector_poll: acquire attention outbox leases"
        ),
        ("connector_ack", 0): (
            "tendwire.connector_ack: acknowledge duplicate leased attention"
        ),
        ("send_message", 0): (
            "telegram.send_message: deliver leased attention to general thread"
        ),
        ("send_message", 1): (
            "telegram.send_message: deliver leased attention to root fallback"
        ),
        ("connector_ack", 1): (
            "tendwire.connector_ack: acknowledge delivered leased attention"
        ),
        ("connector_fail", 0): (
            "tendwire.connector_fail: fail exact leased attention"
        ),
    }
    delivery_tree = ast.parse(delivery_text)
    delivery_parents = {
        child: parent
        for parent in ast.walk(delivery_tree)
        for child in ast.iter_child_nodes(parent)
    }

    def delivery_function_name(node):
        current = node
        while current in delivery_parents:
            current = delivery_parents[current]
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return current.name
        return ""

    delivery_aliases: set[str] = set()
    for node in ast.walk(delivery_tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        targets = (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        if not (
            isinstance(value, ast.Attribute)
            and root_name(value.value) in {"telegram", "tendwire"}
            and value.attr not in read_only
            and value.attr not in {"get", "setdefault"}
        ):
            continue
        delivery_aliases.update(
            target.id for target in targets if isinstance(target, ast.Name)
        )

    occurrences: dict[str, int] = {}
    observed_exact: set[tuple[str, int]] = set()
    for node in ast.walk(delivery_tree):
        if not isinstance(node, ast.Call):
            continue
        owner = delivery_function_name(node)
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in delivery_aliases
        ):
            violations.append(
                ("delivery_raw_mutation_alias", node.lineno, owner)
            )
            continue
        if (
            call_name(node.func) == "getattr"
            and node.args
            and root_name(node.args[0]) in {"telegram", "tendwire"}
        ):
            violations.append(
                ("delivery_raw_getattr", node.lineno, owner)
            )
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr not in read_only
            and node.func.attr not in {"get", "setdefault"}
            and root_name(node.func.value) in {"telegram", "tendwire"}
        ):
            continue
        if owner != "drain_outbox":
            violations.append(
                ("delivery_raw_mutation", node.lineno, owner, node.func.attr)
            )
            continue
        name = node.func.attr
        ordinal = occurrences.get(name, 0)
        occurrences[name] = ordinal + 1
        key = (name, ordinal)
        observed_exact.add(key)
        reason = exact_reasons.get(key)
        capability = (
            "tendwire." if name.startswith("connector_") else "telegram."
        ) + name
        if not reason or capability not in reason:
            violations.append(
                ("undeclared_exact_consumer", node.lineno, name, ordinal)
            )
        else:
            reasons.append(reason)
    if observed_exact != set(exact_reasons):
        violations.append(
            ("exact_consumer_inventory", observed_exact, set(exact_reasons))
        )
    decisions_tree = ast.parse(decisions_text)
    decision_parents = {
        child: parent
        for parent in ast.walk(decisions_tree)
        for child in ast.iter_child_nodes(parent)
    }

    def decision_function_name(node):
        current = node
        while current in decision_parents:
            current = decision_parents[current]
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return current.name
        return ""

    decision_aliases: set[str] = set()
    for node in ast.walk(decisions_tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        targets = (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        if not (
            isinstance(value, ast.Attribute)
            and root_name(value.value) in {"telegram", "tendwire"}
            and value.attr not in read_only
        ):
            continue
        decision_aliases.update(
            target.id for target in targets if isinstance(target, ast.Name)
        )

    direct_decision_capabilities = {
        ("telegram", "send_message"),
        ("telegram", "edit_message"),
        ("telegram", "edit_message_reply_markup"),
        ("telegram", "delete_message"),
        ("tendwire", "command"),
    }
    for node in ast.walk(decisions_tree):
        owner = decision_function_name(node)
        if isinstance(node, ast.Attribute) and (
            node.attr == "_client"
            or node.attr.startswith("_OfflockClient__provider")
        ):
            violations.append(
                ("decision_raw_client_attribute", node.lineno, owner)
            )
        if not isinstance(node, ast.Call):
            continue
        name = call_name(node.func)
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in decision_aliases
        ):
            violations.append(
                ("decision_raw_mutation_alias", node.lineno, owner)
            )
        if (
            name == "getattr"
            and node.args
            and root_name(node.args[0]) in {"telegram", "tendwire"}
        ):
            violations.append(
                ("decision_raw_getattr", node.lineno, owner)
            )
        if (
            isinstance(node.func, ast.Attribute)
            and root_name(node.func.value) in {"telegram", "tendwire"}
            and name not in read_only
        ):
            provider = root_name(node.func.value)
            if (
                owner != "_execute_direct"
                or (provider, name) not in direct_decision_capabilities
            ):
                violations.append(
                    ("decision_raw_mutation", node.lineno, owner, name)
                )
        if name == "_operation":
            capability = (
                node.args[0].value
                if node.args
                and isinstance(node.args[0], ast.Constant)
                else None
            )
            reason = literal_keyword(node, "reason")
            if (
                not isinstance(capability, str)
                or not isinstance(reason, str)
                or capability not in reason
            ):
                violations.append(
                    ("decision_capability_reason", node.lineno)
                )
            else:
                reasons.append(reason)

    direct_dispatcher = next(
        node
        for node in decisions_tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_execute_direct"
    )
    for node in ast.walk(direct_dispatcher):
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Store)
            and isinstance(node.value, ast.Name)
            and node.value.id in {"entry", "record", "store"}
        ):
            violations.append(
                ("decision_dispatcher_state_write", node.lineno)
            )

    # Rich adapters execute inside the raw-provider closure.  Their provider
    # result may describe a state transition, but the callback itself must not
    # mutate the live telegram state that will be discarded by reload.
    rich_tree = ast.parse(rich_text)
    rich_parents = {
        child: parent
        for parent in ast.walk(rich_tree)
        for child in ast.iter_child_nodes(parent)
    }

    def rich_function_name(node):
        current = node
        while current in rich_parents:
            current = rich_parents[current]
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return current.name
        return ""

    adapter_roots = {
        "edit_feed_item",
        "edit_rich_message",
        "edit_turn_delivery_part",
        "send_feed_item",
        "send_rich_message",
        "send_turn_delivery_part",
    }
    rich_functions = {
        node.name: node
        for node in rich_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    reachable = set(adapter_roots)
    changed = True
    while changed:
        changed = False
        for name in list(reachable):
            function = rich_functions.get(name)
            if function is None:
                continue
            for node in ast.walk(function):
                called = (
                    node.func.id
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    else ""
                )
                if called in rich_functions and called not in reachable:
                    reachable.add(called)
                    changed = True
    rich_aliases: dict[str, set[str]] = {}
    for owner in reachable:
        function = rich_functions.get(owner)
        if function is None:
            continue
        aliases = rich_aliases.setdefault(owner, set())
        for node in ast.walk(function):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
            )
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "rich_telegram_state"
                and value.args
                and root_name(value.args[0]) == "telegram"
            ):
                aliases.update(
                    target.id
                    for target in targets
                    if isinstance(target, ast.Name)
                )
    for node in ast.walk(rich_tree):
        owner = rich_function_name(node)
        if owner not in reachable:
            continue
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Store)
            and (
                root_name(node.value) == "telegram"
                or root_name(node.value) in rich_aliases.get(owner, set())
            )
        ):
            violations.append(
                ("rich_adapter_state_write", node.lineno, owner)
            )
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and (
                root_name(node.func.value) == "telegram"
                or root_name(node.func.value)
                in rich_aliases.get(owner, set())
            )
            and node.func.attr in {
                "clear",
                "pop",
                "setdefault",
                "update",
            }
        ):
            violations.append(
                ("rich_adapter_state_write", node.lineno, owner)
            )

    def public_methods(text, class_name):
        tree = ast.parse(text)
        cls = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == class_name
        )
        return {
            node.name
            for node in cls.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not node.name.startswith("_")
        }

    telegram_known = set(read_only_by_provider["telegram"]) | {"api"} | {
        capability.split(".", 1)[1]
        for capability in capabilities
        if capability.startswith("telegram.")
    }
    tendwire_known = set(read_only_by_provider["tendwire"]) | {
        capability.split(".", 1)[1]
        for capability in capabilities
        if capability.startswith("tendwire.")
    }
    for provider, methods, known in (
        (
            "telegram",
            public_methods(delivery_text, "TelegramClient"),
            telegram_known,
        ),
        (
            "tendwire",
            public_methods(tendwire_text, "TendwireClient"),
            tendwire_known,
        ),
    ):
        for method in sorted(methods - known):
            violations.append(
                ("unclassified_provider_method", provider, method)
            )

    duplicates = sorted(
        reason for reason in set(reasons) if reasons.count(reason) > 1
    )
    if duplicates:
        violations.append(("duplicate_capability_reason", tuple(duplicates)))
    return violations


def test_source_sync_mutations_require_offlock_executor():
    """The off-lock protocol is enforced structurally, not by convention."""

    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    assert _offlock_protocol_violations(source_text, delivery_text) == []


def test_offlock_enforcement_rejects_all_three_demonstrated_bypasses():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")

    tendwire_escape = source_text.replace(
        "topic_name = entry.get(\"topic_name\")",
        (
            "runtime.tendwire.command(\"unsafe\")\n"
            "    entry[\"last_topic_error\"] = \"unsafe\"\n"
            "    topic_name = entry.get(\"topic_name\")"
        ),
        1,
    )
    assert any(
        violation[0] == "raw_mutation"
        for violation in _offlock_protocol_violations(
            tendwire_escape, delivery_text
        )
    )
    aliased_escape = source_text.replace(
        "topic_name = entry.get(\"topic_name\")",
        (
            "unsafe_command = runtime.tendwire.command\n"
            "    unsafe_command(\"unsafe\")\n"
            "    entry[\"last_topic_error\"] = \"unsafe\"\n"
            "    topic_name = entry.get(\"topic_name\")"
        ),
        1,
    )
    assert any(
        violation[0] == "raw_mutation_alias"
        for violation in _offlock_protocol_violations(
            aliased_escape, delivery_text
        )
    )
    getattr_escape = source_text.replace(
        "topic_name = entry.get(\"topic_name\")",
        (
            "getattr(runtime.tendwire, \"command\")(\"unsafe\")\n"
            "    entry[\"last_topic_error\"] = \"unsafe\"\n"
            "    topic_name = entry.get(\"topic_name\")"
        ),
        1,
    )
    assert any(
        violation[0] == "raw_getattr_mutation"
        for violation in _offlock_protocol_violations(
            getattr_escape, delivery_text
        )
    )

    callback_escape = source_text.replace(
        "if mutation.capability == \"telegram.send_voice_batch\":",
        (
            "if mutation.capability == \"telegram.send_voice_batch\":\n"
            "            entry = {}\n"
            "            provider.send_message(\"-100\", \"unsafe\")\n"
            "            entry[\"last_topic_error\"] = \"unsafe\""
        ),
        1,
    )
    callback_violations = _offlock_protocol_violations(
        callback_escape, delivery_text
    )
    assert any(
        violation[0] == "dispatcher_extra_mutation"
        for violation in callback_violations
    )
    assert any(
        violation[0] == "dispatcher_state_write"
        for violation in callback_violations
    )

    class MutatingTendwire:
        calls = 0

        def command(self, *_args, **_kwargs):
            self.calls += 1

        command_json = command
        call = command

    tendwire = MutatingTendwire()
    assert {
        "tendwire.call",
        "tendwire.command",
        "tendwire.command_json",
        "tendwire.connector_poll",
        "tendwire.turn_final_poll",
    } <= set(source_sync._DIRECT_PROVIDER_CAPABILITIES)
    tendwire_reads = source_sync._READ_ONLY_PROVIDER_METHODS["tendwire"]
    assert "connector_poll" not in tendwire_reads
    assert "turn_final_poll" not in tendwire_reads
    guarded_tendwire = source_sync._OfflockClient(
        tendwire, _store(), "tendwire"
    )
    for method_name in ("command", "command_json", "call"):
        with pytest.raises(
            RuntimeError, match="requires _execute_entry_operation"
        ):
            getattr(guarded_tendwire, method_name)("unsafe")
    assert tendwire.calls == 0

    telegram = FakeTelegram()
    guarded_telegram = source_sync._OfflockClient(
        telegram, _store(), "telegram"
    )
    with pytest.raises(AttributeError, match="raw provider state"):
        guarded_telegram._client.send_message("-100", "unsafe")
    with pytest.raises(AttributeError, match="raw provider state"):
        getattr(guarded_telegram, "_client").send_message("-100", "unsafe")
    assert telegram.sent == []


def test_offlock_enforcement_fails_closed_for_unknown_and_lease_methods():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    tendwire_path = source_path.with_name("tendwire_client.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    tendwire_text = tendwire_path.read_text(encoding="utf-8")

    invented_telegram = delivery_text.replace(
        "class TelegramClient:",
        (
            "class TelegramClient:\n"
            "    def ban_user(self, user_id):\n"
            "        return {\"ok\": True, \"user_id\": user_id}\n"
        ),
        1,
    )
    invented_tendwire = tendwire_text.replace(
        "class TendwireClient:",
        (
            "class TendwireClient:\n"
            "    def mutate_future_state(self):\n"
            "        return {\"ok\": True}\n"
        ),
        1,
    )
    assert (
        "unclassified_provider_method",
        "telegram",
        "ban_user",
    ) in _offlock_protocol_violations(
        source_text, invented_telegram, tendwire_text=tendwire_text
    )
    assert (
        "unclassified_provider_method",
        "tendwire",
        "mutate_future_state",
    ) in _offlock_protocol_violations(
        source_text, delivery_text, tendwire_text=invented_tendwire
    )

    for method in ("connector_poll", "turn_final_poll"):
        lease_escape = source_text.replace(
            "topic_name = entry.get(\"topic_name\")",
            (
                f"runtime.tendwire.{method}()\n"
                "    entry[\"last_topic_error\"] = \"unsafe\"\n"
                "    topic_name = entry.get(\"topic_name\")"
            ),
            1,
        )
        lease_violations = _offlock_protocol_violations(
            lease_escape, delivery_text
        )
        assert any(
            violation[0] == "raw_mutation"
            and violation[2:] == ("_ensure_topic", method)
            for violation in lease_violations
        )

    class FutureProvider:
        calls = 0

        def ban_user(self):
            self.calls += 1
            return {"ok": True}

    provider = FutureProvider()
    guarded = source_sync._OfflockClient(
        provider, _store(), "telegram"
    )
    with pytest.raises(
        RuntimeError, match="requires _execute_entry_operation"
    ):
        guarded.ban_user()
    assert provider.calls == 0


def test_offlock_internal_entrypoints_reject_bypass_and_capability_replay():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    for injected, expected in (
        (
            "runtime.telegram._execute_entry(None, None)",
            "internal_executor_escape",
        ),
        (
            "source_sync._invoke_offlock(runtime.telegram, lambda p: None)",
            "internal_executor_escape",
        ),
        (
            (
                "object.__getattribute__(runtime.telegram, "
                "\"_OfflockClient__provider\").send_message("
                "\"-100\", \"unsafe\")"
            ),
            "reflected_raw_provider",
        ),
    ):
        escaped = source_text.replace(
            "topic_name = entry.get(\"topic_name\")",
            (
                f"{injected}\n"
                "    entry[\"last_topic_error\"] = \"unsafe\"\n"
                "    topic_name = entry.get(\"topic_name\")"
            ),
            1,
        )
        assert any(
            violation[0] == expected
            for violation in _offlock_protocol_violations(
                escaped, delivery_text
            )
        )

    telegram = FakeTelegram()
    guarded = source_sync._OfflockClient(
        telegram, _store(), "telegram"
    )
    capability = source_sync._provider_mutation(
        "telegram.send_message",
        reason="telegram.send_message: single-use replay regression",
        args=("-100", "once"),
    )
    with pytest.raises(RuntimeError, match="private to audited wrappers"):
        guarded._execute_exact(capability)
    assert telegram.sent == []

    first = source_sync._execute_exact_provider_operation(
        guarded, mutation=capability
    )
    assert first["ok"] is True
    with pytest.raises(RuntimeError, match="already consumed"):
        source_sync._execute_exact_provider_operation(
            guarded, mutation=capability
        )
    assert len(telegram.sent) == 1


def test_offlock_alias_computed_method_escape_is_rejected_and_stale_refs_detach(
    tmp_path, monkeypatch
):
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    escaped = source_text.replace(
        "topic_name = entry.get(\"topic_name\")",
        (
            "unsafe_executor = _OFFLOCK_EXECUTOR.read\n"
            "    method_name = \"\".join([\"send\", \"_message\"])\n"
            "    unsafe_executor(runtime.telegram, method_name, "
            "\"-100\", \"unsafe\")\n"
            "    dynamic_get = getattr\n"
            "    dynamic_get(runtime.telegram, method_name)("
            "\"-100\", \"unsafe-again\")\n"
            "    reflect = object.__getattribute__\n"
            "    raw_name = \"\".join([\"_OfflockClient__\", "
            "\"provider\"])\n"
            "    reflect(runtime.telegram, raw_name)\n"
            "    entry[\"last_topic_error\"] = \"unsafe\"\n"
            "    topic_name = entry.get(\"topic_name\")"
        ),
        1,
    )
    violations = _offlock_protocol_violations(escaped, delivery_text)
    assert any(row[0] == "executor_alias_escape" for row in violations)
    assert sum(row[0] == "sensitive_alias_escape" for row in violations) >= 2
    assert "_OFFLOCK_EXECUTOR_TOKEN" not in source_text
    assert "_invoke_offlock" not in source_text

    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    store = _store()
    store["panes"]["worker-1"] = {
        "source": "tendwire",
        "entry_type": "worker",
        "tendwire_worker_id": "worker-1",
        "tendwire_stable_key": (
            "wsk1_" + hashlib.sha256(b"worker-1").hexdigest()
        ),
        "tendwire_stable_key_version": 1,
        "topic_id": "77",
    }
    state.save_state(store, state_path)

    class RebindingReadProvider:
        def configured(self):
            concurrent = state.load_state(state_path)
            concurrent["panes"]["worker-1"]["topic_id"] = "88"
            state.save_state(concurrent, state_path)
            return True

    with state.state_lock(state_path):
        current = state.load_state(state_path)
        stale_entry = current["panes"]["worker-1"]
        guarded = source_sync._OfflockClient(
            RebindingReadProvider(), current, "telegram"
        )
        assert guarded.configured() is True
        reloaded_entry = current["panes"]["worker-1"]
        assert stale_entry is not reloaded_entry
        stale_entry["stale_write_on_current"] = "must-not-land"

    assert reloaded_entry["topic_id"] == "88"
    assert "stale_write_on_current" not in reloaded_entry


def test_offlock_invariant_scans_decision_consumers_and_dispatcher():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    decisions_path = source_path.with_name("decisions.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    decisions_text = decisions_path.read_text(encoding="utf-8")

    raw_consumer = decisions_text.replace(
        (
            '    """Reconcile active inline keyboards with one '
            'already-fetched pending list."""'
        ),
        (
            '    """Reconcile active inline keyboards with one '
            'already-fetched pending list."""\n'
            '    telegram.send_message(chat_id, "unsafe")'
        ),
        1,
    )
    assert any(
        violation[0] == "decision_raw_mutation"
        for violation in _offlock_protocol_violations(
            source_text,
            delivery_text,
            decisions_text=raw_consumer,
        )
    )

    laundered_dispatch = decisions_text.replace(
        "    kwargs = dict(operation.kwargs)",
        (
            "    telegram.send_voice(\"-100\", \"unsafe.ogg\")\n"
            "    kwargs = dict(operation.kwargs)"
        ),
        1,
    )
    assert any(
        violation[0] == "decision_raw_mutation"
        for violation in _offlock_protocol_violations(
            source_text,
            delivery_text,
            decisions_text=laundered_dispatch,
        )
    )


def test_offlock_invariant_rejects_rich_adapter_state_writes():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    rich_path = source_path.with_name("rich_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    rich_text = rich_path.read_text(encoding="utf-8")
    escaped = rich_text.replace(
        "    target = _client_for_token(client, api_token)",
        (
            "    rich = rich_telegram_state(telegram)\n"
            "    rich[\"supported\"] = \"no\"\n"
            "    target = _client_for_token(client, api_token)"
        ),
        1,
    )
    assert any(
        row[0] == "rich_adapter_state_write"
        for row in _offlock_protocol_violations(
            source_text,
            delivery_text,
            rich_text=escaped,
        )
    )


def test_offlock_invariant_rejects_duplicate_and_mismatched_reasons():
    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")

    duplicate = source_text.replace(
        "telegram.send_message: create global status in root fallback",
        "telegram.send_message: create global status in general thread",
        1,
    )
    assert any(
        violation[0] == "duplicate_capability_reason"
        for violation in _offlock_protocol_violations(
            duplicate, delivery_text
        )
    )

    mismatched = source_text.replace(
        "telegram.create_topic: mint missing pane topic",
        "telegram.send_message: mint missing pane topic",
        1,
    )
    assert any(
        violation[0] == "reason_does_not_name_capability"
        for violation in _offlock_protocol_violations(
            mismatched, delivery_text
        )
    )


def _gateway_child(
    request_id,
    *,
    checkpoint=herdres_gateway.CHECKPOINT_ADVANCE,
    disposition=None,
    reply="",
    handled=True,
):
    transport = (
        "written_to_pty" if disposition == "terminal_accepted" else disposition
    )
    if checkpoint == herdres_gateway.CHECKPOINT_RETRY:
        request_phase = "retryable"
        terminal_outcome = None
        reply = ""
    elif disposition == "terminal_accepted":
        checkpoint = herdres_gateway.CHECKPOINT_HOLD
        request_phase = "accepted_unverified"
        terminal_outcome = "delivery_unknown"
        reply = ""
    elif disposition == "terminal_rejected":
        checkpoint = herdres_gateway.CHECKPOINT_HOLD
        request_phase = "terminal"
        terminal_outcome = "not_delivered"
        reply = ""
    elif disposition == "terminal_uncertain":
        checkpoint = herdres_gateway.CHECKPOINT_HOLD
        request_phase = "terminal"
        terminal_outcome = "delivery_unknown"
    else:
        request_phase = "terminal"
        terminal_outcome = "delivered"
        transport = "submitted" if transport is None else transport
    return {
        "schema_version": 2,
        "handled": handled,
        "request_id": request_id,
        "checkpoint": checkpoint,
        "transport_disposition": transport,
        "request_phase": request_phase,
        "terminal_outcome": terminal_outcome,
        "reply": reply,
    }


def _source_worker(worker, *, stable_identity=True):
    """Return a test worker with a deterministic valid identity by default."""
    result = dict(worker)
    meta = dict(result.get("meta") or {})
    if (
        stable_identity
        and "stable_key" not in meta
        and "stable_key_version" not in meta
    ):
        material = f"{result.get('id') or ''}\0{result.get('fingerprint') or ''}"
        meta["stable_key"] = "wsk1_" + hashlib.sha256(material.encode()).hexdigest()
        meta["stable_key_version"] = 1
    result["meta"] = meta
    return result


def _accepted_command_response(request):
    worker_id = str(request.get("target", {}).get("worker_id") or "worker-1")
    return {
        "schema_version": 2,
        "action": "send_instruction",
        "request_id": request["request_id"],
        "ok": True,
        "dry_run": False,
        "status": "accepted",
        "disposition": "terminal_accepted",
        "result": {
            "target": {"worker_id": worker_id},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": "idle",
            "observed_turn_state": "pending_observation",
        },
        "error": None,
        "warnings": [],
    }
def _failed_command_response(request, *, status, disposition):
    return {
        "schema_version": 2,
        "action": "send_instruction",
        "request_id": request["request_id"],
        "ok": False,
        "dry_run": False,
        "status": status,
        "disposition": disposition,
        "result": None,
        "error": {"code": status, "message": "public command failure"},
        "warnings": [],
    }




class FakeTendwire:
    def __init__(
        self,
        *,
        turns=None,
        pending=None,
        workers=None,
        spaces=None,
        stable_identities=True,
    ):
        self.commands = []
        self._turns = dict(turns) if turns is not None else {"turns": []}
        self._turns.setdefault("schema_version", 1)
        self._pending = pending if pending is not None else {"pending_interactions": []}
        raw_workers = workers if workers is not None else [
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "fp-1",
                "meta": {"agent": "codex"},
            }
        ]
        self._workers = [
            _source_worker(worker, stable_identity=stable_identities)
            for worker in raw_workers
        ]
        self._spaces = spaces if spaces is not None else [
            {
                "id": "space-1",
                "name": "Project",
                "status": "active",
                "fingerprint": "space-fp-1",
            }
        ]

    def snapshot(self):
        return {
            "ok": True,
            "spaces": self._spaces,
            "workers": self._workers,
        }

    def turns(self):
        return self._turns

    def pending(self):
        return self._pending

    def connector_poll(self, **_kwargs):
        return {"ok": True, "items": []}

    def command(self, request):
        self.commands.append(request)
        return _accepted_command_response(request)

    def command_json(self, request_json):
        request = json.loads(request_json)
        self.commands.append(request)
        return _accepted_command_response(request)


class FakeTelegram:
    dry_run = False

    def __init__(self, token="fake", shared=None):
        self.token = token
        shared = shared or {
            "sent": [],
            "edited": [],
            "topics": [],
            "deleted_topics": [],
            "closed_topics": [],
            "reopened_topics": [],
            "renamed_topics": [],
            "pins": [],
            "api_calls": [],
            "icon_edits": [],
            "voice_notes": [],
        }
        shared.setdefault("voice_notes", [])
        shared.setdefault("renamed_topics", [])
        shared.setdefault("closed_topics", [])
        shared.setdefault("reopened_topics", [])
        self._shared = shared
        self.sent = shared["sent"]
        self.edited = shared["edited"]
        self.topics = shared["topics"]
        self.deleted_topics = shared["deleted_topics"]
        self.closed_topics = shared["closed_topics"]
        self.reopened_topics = shared["reopened_topics"]
        self.renamed_topics = shared["renamed_topics"]
        self.pins = shared["pins"]
        self.api_calls = shared["api_calls"]
        self.icon_edits = shared["icon_edits"]
        self.voice_notes = shared["voice_notes"]

    def with_token(self, token):
        return FakeTelegram(token=token, shared=self._shared)

    def api(self, method, payload):
        self.api_calls.append((method, dict(payload), self.token))
        if method == "sendRichMessage":
            message_id = str(100 + len(self.sent))
            rich = json.loads(payload.get("rich_message") or "{}")
            kwargs = {
                "thread_id": str(payload.get("message_thread_id") or ""),
                "format": "rich",
                "token": self.token,
            }
            self.sent.append((str(payload.get("chat_id") or ""), str(rich.get("html") or ""), kwargs, message_id))
            return {"ok": True, "result": {"message_id": message_id}}
        if method == "editMessageText":
            rich_payload = payload.get("rich_message")
            rich = json.loads(rich_payload) if rich_payload else {}
            html = str(rich.get("html") or payload.get("text") or "")
            self.edited.append((str(payload.get("chat_id") or ""), str(payload.get("message_id") or ""), html))
            return {"ok": True, "result": {"message_id": str(payload.get("message_id") or "0")}}
        if method == "getForumTopicIconStickers":
            return {
                "ok": True,
                "result": [
                    {"emoji": "⚡️", "custom_emoji_id": "icon-working"},
                    {"emoji": "✅", "custom_emoji_id": "icon-idle"},
                    {"emoji": "❓", "custom_emoji_id": "icon-attention"},
                    {"emoji": "‼️", "custom_emoji_id": "icon-failed"},
                    {"emoji": "🦊", "custom_emoji_id": "icon-fox"},
                ],
            }
        return {"ok": True, "result": {"message_id": 0}}

    def create_topic(self, _chat_id, name, icon_color=None):
        self.topics.append(name)
        return {"ok": True, "topic_id": str(76 + len(self.topics))}

    def rename_topic(self, chat_id, thread_id, name):
        self.renamed_topics.append((str(chat_id), str(thread_id), str(name)))
        return {"ok": True}

    def edit_topic_icon(self, chat_id, thread_id, emoji_id):
        self.icon_edits.append((str(chat_id), str(thread_id), str(emoji_id)))
        return {"ok": True}

    def delete_topic(self, _chat_id, thread_id):
        self.deleted_topics.append(str(thread_id))
        return {"ok": True}

    def close_topic(self, _chat_id, thread_id):
        self.closed_topics.append(str(thread_id))
        return {"ok": True}

    def reopen_topic(self, _chat_id, thread_id):
        self.reopened_topics.append(str(thread_id))
        return {"ok": True}

    def send_message(self, chat_id, html, **kwargs):
        message_id = str(100 + len(self.sent))
        payload_kwargs = dict(kwargs)
        payload_kwargs["token"] = self.token
        self.sent.append((chat_id, html, payload_kwargs, message_id))
        return {"ok": True, "message_id": message_id}

    def edit_message(self, chat_id, message_id, html):
        self.edited.append((chat_id, str(message_id), html))
        return {"ok": True, "message_id": str(message_id)}

    def pin_message(self, chat_id, message_id):
        self.pins.append((chat_id, str(message_id)))
        return {"ok": True}

    def send_voice(self, chat_id, file_path, **kwargs):
        message_id = str(900 + len(self.voice_notes))
        self.voice_notes.append((str(chat_id), str(file_path), dict(kwargs), message_id))
        return {"ok": True, "message_id": message_id}


class RebindingNotificationTelegram(FakeTelegram):
    def __init__(self, state_path, mutate_state):
        super().__init__()
        self.state_path = state_path
        self.mutate_state = mutate_state
        self.rebound = False
        self.deleted_messages = []

    def send_message(self, chat_id, html, **kwargs):
        result = super().send_message(chat_id, html, **kwargs)
        if not self.rebound:
            concurrent = state.load_state(self.state_path)
            self.mutate_state(concurrent)
            state.save_state(concurrent, self.state_path)
            self.rebound = True
        return result

    def delete_message(self, chat_id, message_id):
        self.deleted_messages.append((str(chat_id), str(message_id)))
        return {"ok": True}


class CrashNotificationTelegram(FakeTelegram):
    def __init__(self):
        super().__init__()
        self.deleted_messages = []

    def delete_message(self, chat_id, message_id):
        self.deleted_messages.append((str(chat_id), str(message_id)))
        return {"ok": True}


def _store():
    return {
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "panes": {},
        "spaces": {},
        "tendwired_bootstrap_complete": True,
    }


def test_sync_once_wires_topic_lifecycle_cleanup(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    calls = []

    def lifecycle(current, runtime, *, chat_id, now=None):
        calls.append((current, runtime.telegram, chat_id, now))
        return source_sync._topic_cleanup_empty_result()

    monkeypatch.setattr(
        source_sync, "_sync_topic_lifecycle_cleanup", lifecycle
    )
    store = _store()
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
    )

    assert result["ok"] is True
    assert len(calls) == 1
    assert calls[0][0] is store
    assert calls[0][1] is telegram
    assert calls[0][2] == "-100"
    assert isinstance(calls[0][3], float)


def test_status_overview_uses_old_pane_board_shape():
    html = render_status_overview(
        [
            {
                "agent": "claude",
                "worker_name": "claude",
                "model": "claude-opus-4-8",
                "status": "idle",
            },
            {
                "agent": "codex",
                "worker_name": "codex",
                "model": "gpt-5-codex",
                "status": "working",
            }
        ]
    )

    assert html.splitlines() == ["Codex · GPT-5 Codex 🟡", "Claude · Opus 4.8 🟢"]
    assert "Herdres · Tendwire source mode" not in html
    assert "active:" not in html
    assert "no active pane" not in html.lower()


def test_status_overview_disambiguates_duplicate_agent_labels():
    html = render_status_overview(
        [
            {"agent": "codex", "worker_name": "codex", "tendwire_worker_id": "codex", "status": "idle"},
            {"agent": "codex", "worker_name": "codex", "tendwire_worker_id": "codex-1-2", "status": "idle"},
        ]
    )

    assert html.splitlines() == ["Codex 🟢", "Codex 1-2 🟢"]


def test_source_working_turn_renders_working_not_response():
    item = turn_item_from_source(
        {
            "id": "turn-working",
            "worker_id": "worker-1",
            "assistant_stream_text": "I am checking the current path.",
            "complete": False,
        },
        {"topic_name": "Project"},
    )
    html = render_turn_item_html(item)

    assert item["assistant_final_text"] == ""
    assert item["worklog_text"] == "I am checking the current path."
    assert "✅ Response" not in html
    assert "Working" in html
    assert "I am checking the current path." in html


def test_source_completed_stream_only_turn_can_render_response():
    item = turn_item_from_source(
        {
            "id": "turn-final",
            "worker_id": "worker-1",
            "assistant_stream_text": "Final text from a completed stream-only turn.",
            "complete": True,
        },
        {"topic_name": "Project"},
    )
    html = render_turn_item_html(item)

    assert item["assistant_final_text"] == "Final text from a completed stream-only turn."
    assert item["worklog_text"] == ""
    assert "✅ Response" in html
    assert "Final text from a completed stream-only turn." in html


def test_first_sync_bootstraps_current_turns_without_telegram_posts(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    store.pop("tendwired_bootstrap_complete", None)
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-0",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "user_text": "Old prompt",
                "assistant_final_text": "Old final",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["bootstrap_seen"] == 1
    assert result["feed_sent"] == 0
    assert not any("Old final" in sent[1] for sent in telegram.sent)
    assert store["tendwired_bootstrap_complete"] is True


@pytest.mark.parametrize(
    ("schema_version", "received"),
    [
        pytest.param(..., None, id="missing"),
        pytest.param(True, True, id="bool-true"),
        pytest.param(False, False, id="bool-false"),
        pytest.param("1", "1", id="string"),
        pytest.param("x" * 200, "x" * 80, id="bounded-string"),
        pytest.param(1.0, 1.0, id="float"),
        pytest.param([], None, id="list"),
        pytest.param({}, None, id="mapping"),
    ],
)
def test_invalid_turn_schema_preflight_fails_before_all_mutation(
    monkeypatch, schema_version, received
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    store["continuity_sentinel"] = {"topic_id": "77", "pin_id": "55"}
    before = json.loads(json.dumps(store))
    telegram = FakeTelegram()
    tendwire = FakeTendwire()
    tendwire._turns = {"turns": []}
    if schema_version is not ...:
        tendwire._turns["schema_version"] = schema_version

    result = sync_once(
        store, SyncRuntime(tendwire, telegram, with_outbox=False)
    )

    assert result == {
        "ok": False,
        "status": "unsupported_turn_schema_version",
        "changed": False,
        "created": 0,
        "updated": 0,
        "panes": 0,
        "spaces": 0,
        "icon_updated": 0,
        "pinned_status_updated": 0,
        "feed_sent": 0,
        "sent": 0,
        "routing_repaired": 0,
        "message_bindings": 0,
        "turn_updates": 0,
        "topic_cleanup": {
            "deleted": 0,
            "failed": 0,
            "pruned": 0,
            "changed": False,
        },
        "content_pages": 0,
        "tendwire_turn_final": {
            "enabled": False,
            "polled": 0,
            "operations": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "uncertain": 0,
            "changed": False,
        },
        "tendwire_outbox": {
            "enabled": False,
            "polled": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "changed": False,
        },
        "required_turn_schema_version": 1,
        "received_turn_schema_version": received,
    }
    assert store == before
    assert telegram.sent == []
    assert telegram.edited == []
    assert telegram.topics == []
    assert telegram.renamed_topics == []
    assert telegram.deleted_topics == []
    assert telegram.pins == []
    assert telegram.icon_edits == []
    assert telegram.voice_notes == []
    assert telegram.api_calls == []


def test_sync_delivers_final_turn_once(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "user_text": "Question",
                "assistant_final_text": "Full final answer",
                "complete": True,
            }
        ]
    }
    runtime = SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False)

    first = sync_once(store, runtime)
    second = sync_once(store, runtime)

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert any("Full final answer" in sent[1] for sent in telegram.sent)
    assert any(sent[2]["thread_id"] == "77" for sent in telegram.sent if "Full final answer" in sent[1])
    assert len(store["tendwire_source_delivered_turns"]) == 1
    response_message_id = [sent[3] for sent in telegram.sent if "Full final answer" in sent[1]][0]
    binding = state.find_message_binding(store, response_message_id, topic_id="77")
    assert binding is not None
    assert binding["worker_id"] == "worker-1"


def test_source_final_uses_configured_managed_bot_voice(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    _key, stale, _created = state.upsert_worker_entry(store, _source_worker({
        "id": "worker-1",
        "name": "codex",
        "status": "working",
        "space_id": "space-1",
        "fingerprint": "fp-1",
        "meta": {"agent": "codex"},
    }), )
    stale["managed_bot_kind"] = "claude"
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Codex final",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["feed_sent"] == 1
    assert any(
        "Codex final" in sent[1] and sent[2]["token"] == "codex-token"
        for sent in telegram.sent
    )
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_bot_kind"] == "codex"
    binding = state.find_message_binding(store, entry["last_clean_message_id"], topic_id="77")
    assert binding is not None
    assert binding["bot_kind"] == "codex"


def test_source_voice_shared_uses_manager_bot_even_when_token_configured(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    _space_key, space, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    space["voice_mode"] = "shared"
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Codex final",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["feed_sent"] == 1
    assert not any(call[0] == "sendRichMessage" and call[2] == "codex-token" for call in telegram.api_calls)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["managed_voice_active"] is False
    assert entry["last_clean_bot_kind"] == "manager"


def test_per_agent_bot_reply_targets_original_worker_once(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    store["telegram"]["managed_bots"] = {
        "claude": {"enabled": True, "token": "claude-token"},
        "codex": {"enabled": True, "token": "codex-token"},
    }
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-claude",
                    "worker_id": "worker-claude",
                    "worker_fingerprint": "fp-claude",
                    "assistant_final_text": "Claude final",
                    "complete": True,
                },
                {
                    "id": "turn-codex",
                    "worker_id": "worker-codex",
                    "worker_fingerprint": "fp-codex",
                    "assistant_final_text": "Codex final",
                    "complete": True,
                },
            ]
        },
        workers=[
            {
                "id": "worker-claude",
                "name": "claude",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-claude",
                "meta": {"agent": "claude"},
            },
            {
                "id": "worker-codex",
                "name": "codex",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-codex",
                "meta": {"agent": "codex"},
            },
        ],
    )

    result = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    state.save_state(store)
    claude_message_id = next(sent[3] for sent in telegram.sent if "Claude final" in sent[1])
    codex_message_id = next(sent[3] for sent in telegram.sent if "Codex final" in sent[1])
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    claude_reply = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "9001",
            "request_id": REQUEST_ID,
            "reply_to_message_id": claude_message_id,
            "text": "/send reply to claude",
        }
    )
    codex_reply = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "9002",
            "request_id": REQUEST_ID_2,
            "reply_to_message_id": codex_message_id,
            "text": "/send reply to codex",
        }
    )

    assert result["feed_sent"] == 2
    assert any(
        "Claude final" in sent[1] and sent[2]["token"] == "claude-token"
        for sent in telegram.sent
    )
    assert any(
        "Codex final" in sent[1] and sent[2]["token"] == "codex-token"
        for sent in telegram.sent
    )
    assert claude_reply == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    assert codex_reply == _gateway_child(
        REQUEST_ID_2,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    assert [command["target"] for command in fake.commands] == [
        {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"},
        {"worker_id": "worker-codex", "worker_fingerprint": "fp-codex"},
    ]
    assert [command["instruction"] for command in fake.commands] == [
        {"text": "reply to claude"},
        {"text": "reply to codex"},
    ]


def test_managed_bot_tokens_include_env_and_state_tokens(monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    monkeypatch.setenv("HERDRES_MANAGED_BOT_CODEX_TOKEN", "codex-token")
    telegram = {"managed_bots": {"claude": {"enabled": True, "token": "claude-token"}}}

    records = managed_bot_tokens(telegram)
    by_kind = {kind: (key, token) for key, kind, token in records}

    assert by_kind["codex"][1] == "codex-token"
    assert by_kind["claude"][1] == "claude-token"
    assert managed_bot_kind_for_key(by_kind["codex"][0]) == "codex"


def test_sync_backfills_existing_message_bindings(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"}), )
    worker["last_clean_message_id"] = "555"
    worker["last_turn_id"] = "turn-1"
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )

    result = sync_once(store, SyncRuntime(FakeTendwire(), FakeTelegram(), with_outbox=False))

    assert result["message_bindings"] == 1
    binding = state.find_message_binding(store, "555", topic_id="77")
    assert binding is not None
    assert binding["worker_id"] == "worker-1"


def test_sync_creates_one_topic_per_space_not_per_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    workers = [
        {"id": "worker-1", "name": "codex", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"},
        {"id": "worker-2", "name": "claude", "status": "done", "space_id": "space-1", "fingerprint": "fp-2"},
    ]
    turns = {
        "turns": [
            {"id": "turn-1", "worker_id": "worker-1", "assistant_final_text": "one", "complete": True},
            {"id": "turn-2", "worker_id": "worker-2", "assistant_final_text": "two", "complete": True},
        ]
    }

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                turns=turns,
                workers=workers,
                spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["spaces"] == 1
    assert result["panes"] == 2
    assert telegram.topics == ["Project"]
    assert len(state.source_entries(store)) == 1
    assert len(state.source_worker_entries(store)) == 2
    assert all(sent[2]["thread_id"] == "77" for sent in telegram.sent if sent[1].startswith("<b>Project"))


def test_topic_creation_checkpoints_provider_identity_before_later_sync_work(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    source = {
        "id": "space-1",
        "name": "Project",
        "status": "active",
        "fingerprint": "space-fp",
    }
    _key, entry, _created = state.upsert_space_entry(store, source)
    checkpoints = []
    runtime = SyncRuntime(
        FakeTendwire(),
        telegram,
        with_outbox=False,
        checkpoint=lambda: checkpoints.append(copy.deepcopy(store)),
    )

    needed, created = source_sync._ensure_topic(
        store,
        source,
        entry,
        runtime,
        chat_id="-100",
    )

    assert needed is True and created is True
    assert telegram.topics == ["Project"]
    assert len(checkpoints) == 1
    persisted_entry = next(iter(state.source_space_entries(checkpoints[0]).values()))
    assert persisted_entry["topic_id"] == entry["topic_id"]

    restored = checkpoints[0]
    restored_entry = next(iter(state.source_space_entries(restored).values()))
    needed, created = source_sync._ensure_topic(
        restored,
        source,
        restored_entry,
        runtime,
        chat_id="-100",
    )
    assert needed is False and created is False
    assert telegram.topics == ["Project"]


def test_worker_topic_creation_refuses_second_topic_for_same_stable_owner(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    stable_key = "wsk1_" + "a" * 64
    first_worker = {
        "id": "worker-1",
        "name": "codex",
        "status": "working",
        "space_id": "space-1",
        "fingerprint": "fp-1",
        "meta": {
            "stable_key": stable_key,
            "stable_key_version": 1,
        },
    }
    second_worker = {
        **first_worker,
        "id": "worker-2",
        "fingerprint": "fp-2",
        "meta": {
            "stable_key": "wsk1_" + "b" * 64,
            "stable_key_version": 1,
        },
    }
    _first_key, _first, _created = state.upsert_worker_entry(
        store, first_worker, topic_id="77"
    )
    _second_key, second, _created = state.upsert_worker_entry(
        store, second_worker
    )
    second["tendwire_stable_key"] = stable_key
    second["tendwire_stable_key_version"] = 1
    second_worker["meta"] = {
        "stable_key": stable_key,
        "stable_key_version": 1,
    }
    telegram = FakeTelegram()

    needed, created = source_sync._ensure_topic(
        store,
        second_worker,
        second,
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
        chat_id="-100",
    )

    assert needed is False and created is False
    assert second.get("topic_id") is None
    assert telegram.topics == []


def test_worker_topic_mode_creates_one_topic_per_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    telegram = FakeTelegram()
    workers = [
        {"id": "worker-1", "name": "codex", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"},
        {"id": "worker-2", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-2"},
    ]

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=workers,
                spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["panes"] == 2
    assert telegram.topics == ["codex", "claude"]
    assert len(state.source_entries(store)) == 2


def test_space_topic_reuses_existing_same_name_worker_topic(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _key, legacy, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-old", "name": "Project", "status": "idle", "space_id": "old-space", "fingerprint": "old-fp"}), topic_id="123",)
    legacy["topic_name"] = "Project"
    telegram = FakeTelegram()

    sync_once(store, SyncRuntime(FakeTendwire(), telegram, with_outbox=False))

    assert telegram.topics == []
    entry = next(iter(state.source_entries(store).values()))
    assert entry["topic_id"] == "123"


def test_space_without_open_worker_is_not_telegram_visible(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "empty-space", "name": "Empty", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["spaces"] == 0
    assert telegram.topics == []
    assert state.source_entries(store) == {}


def test_space_mode_deletes_stale_worker_topics(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _key, stale, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-old", "name": "Old worker", "status": "idle", "space_id": "old-space", "fingerprint": "old-fp"}), topic_id="88",)
    stale["topic_name"] = "Old worker"
    telegram = FakeTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(), telegram, with_outbox=False))

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.deleted_topics == ["88"]
    old = [entry for entry in state.source_worker_entries(store).values() if entry.get("tendwire_worker_id") == "worker-old"][0]
    assert not old.get("topic_id")


def test_finished_council_worker_topic_is_deleted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "gm-1", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-1"}), topic_id="88",)
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[{"id": "gm-1", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-1"}],
                spaces=[{"id": "space-1", "name": "Council", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.topics == []
    assert telegram.deleted_topics == ["88"]
    assert state.source_worker_entries(store) == {}


def test_finished_council_space_topic_is_deleted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert result["topic_cleanup"]["pruned"] == 1
    assert telegram.deleted_topics == ["88"]
    assert state.source_entries(store) == {}


def test_finished_council_worker_and_space_topic_delete_once(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "gm-1", "name": "gm-local-as", "status": "closed", "space_id": "space-1", "fingerprint": "fp-1"}), topic_id="88",)
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.deleted_topics == ["88"]
    assert state.source_entries(store) == {}
    assert state.source_worker_entries(store) == {}


def test_finished_council_worker_does_not_delete_active_space_topic(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "gm-old", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-old"}), topic_id="88",)
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    {"id": "gm-new", "name": "gm-local-as", "status": "working", "space_id": "space-1", "fingerprint": "fp-new"}
                ],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 0
    assert telegram.deleted_topics == []
    assert next(iter(state.source_entries(store).values()))["topic_id"] == "88"


def test_final_response_renders_common_markdown_as_telegram_html():
    html = render_turn_item_html(
        {
            "kind": "turn",
            "title": "Alpha",
            "user_text": "Question",
            "assistant_final_text": "## **Fix it**\n\n- keep **bold**\n- escape <tags>\n\nUse `code`.",
        }
    )

    assert "##" not in html
    assert "**" not in html
    # No redundant top worker title; the Response is the open top-level section.
    assert "<h3>Alpha</h3>" not in html
    assert html.startswith("<b>✅ Response</b><br><br>")
    assert "<h4>Fix it</h4>" in html
    assert "<ul>" in html
    assert "<li>keep <b>bold</b></li>" in html
    assert "escape &lt;tags&gt;" in html
    assert "<code>code</code>" in html
    assert "<p>Use <code>code</code>.</p>" in html
    # Prompt is a de-emphasized (<footer>) collapsible section; no quote bars.
    assert "<details open><summary>💬 <b>You</b></summary><footer>Question</footer></details>" in html
    assert "<blockquote>" not in html
    assert "</details><br><details" not in html


def test_long_final_response_uses_full_visible_response_section():
    html = render_turn_item_html(
        {
            "kind": "turn",
            "title": "Alpha",
            "user_text": "Question",
            "assistant_final_text": "## **Plan**\n\n" + "- keep **rich** sections\n" * 80,
        }
    )

    assert html.startswith("<b>✅ Response</b><br><br>")
    assert "<blockquote>" not in html
    assert "<blockquote expandable>" not in html
    assert "##" not in html
    assert "**" not in html
    assert "<h4>Plan</h4>" in html
    assert "<ul>" in html
    assert "<li>keep <b>rich</b> sections</li>" in html
    assert "</details><br><details" not in html


def test_oversize_rich_response_falls_back_without_raw_markdown_or_truncation(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_MARKER_12345"
    turns = {
        "turns": [
            {
                "id": "turn-huge",
                "worker_id": "worker-1",
                "assistant_final_text": "## **Long**\n\n" + "- keep **rich** sections\n" * 1_100 + tail,
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    sent_text = "\n".join(sent[1] for sent in telegram.sent)
    assert result["feed_sent"] == 1
    assert len(render_turn_item_html({"kind": "turn", "assistant_final_text": turns["turns"][0]["assistant_final_text"]})) > MAX_RICH_HTML_CHARS
    assert any(sent[2].get("format") != "rich" for sent in telegram.sent)
    assert tail in sent_text
    assert "##" not in sent_text
    assert "**" not in sent_text


def test_sync_sends_all_long_final_response_parts(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_MARKER_67890"
    turns = {
        "turns": [
            {
                "id": "turn-long",
                "worker_id": "worker-1",
                "assistant_final_text": "## **Long**\n\n" + "- keep **rich** sections\n" * 700 + tail,
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    response_messages = [sent[1] for sent in telegram.sent if "✅ Response" in sent[1]]
    assert result["feed_sent"] == 1
    assert len(response_messages) >= 1
    assert any(tail in message for message in response_messages)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert len(entry["last_clean_message_ids"]) == len(response_messages)


def _recent_cutoff_response_text():
    return """Fixed both issues and pushed to `origin/tendwired`.

What changed:
- Voice transcription is now enabled locally.
- Installed `sherpa-onnx`, `numpy`, and the Parakeet STT model into `~/.local/share/herdres/speech-venv`.
- `herdres-gateway.service` now prefers that venv in `PATH`.
- Restarted only `herdres-gateway.service`.
- Did not restart Herdr.

The different-bot issue was a real bug: child bot pollers could race on the same unaddressed topic message, and whichever child saw it first could claim the target. Now child bots only handle explicit targets: replies to that bot's message or `@bot` mentions. Normal topic messages go through the manager path and route by active worker/state.

Verification:
- `63 passed`
- `herdres speech check`: `input_enabled=true`, `sherpa_onnx=true`, `stt_model=true`, `ffmpeg=true`
- `herdres doctor`: healthy
- source smoke: `direct_herdr_calls=0`
- `herdr-server.service`: active, status-only checked
- legacy timer: inactive

Pushed:
- `4557d20 Prevent child bot target races`
- branch: `tendwired`"""


def test_medium_final_response_renders_as_single_message():
    # A medium response (~1350 chars rendered) fits one rich message, so it is
    # delivered as a SINGLE part with a plain "Response" marker -- no "1/N" split.
    parts = render_feed_item_delivery_html_parts(
        {"kind": "turn", "assistant_final_text": _recent_cutoff_response_text()}
    )

    assert len(parts) == 1
    assert parts[0].startswith("<b>✅ Response</b><br><br>")
    assert "4557d20 Prevent child bot target races" in parts[0]
    assert "branch: <code>tendwired</code>" in parts[0]


def test_promoted_working_final_edits_in_place_as_single_message(monkeypatch):
    # A medium final (under the legacy edit cap) now promotes the working stream
    # message to the final Response via an in-place EDIT -- one message, no split.
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    working_turns = {
        "turns": [
            {
                "id": "turn-medium",
                "worker_id": "worker-1",
                "assistant_stream_text": "Working on it.",
                "complete": False,
            }
        ]
    }
    final_turns = {
        "turns": [
            {
                "id": "turn-medium",
                "worker_id": "worker-1",
                "assistant_final_text": _recent_cutoff_response_text(),
                "complete": True,
            }
        ]
    }

    sync_once(store, SyncRuntime(FakeTendwire(turns=working_turns), telegram, with_outbox=False))
    result = sync_once(store, SyncRuntime(FakeTendwire(turns=final_turns), telegram, with_outbox=False))

    edited_html = "\n".join(edit[2] for edit in telegram.edited)
    assert result["feed_sent"] == 1
    # The final Response was edited into the existing working message.
    assert "4557d20 Prevent child bot target races" in edited_html
    assert "branch: tendwired" in edited_html
    # Single message -- no "Response i/N" split labels anywhere.
    assert "✅ Response 1/" not in edited_html
    assert "✅ Response 1/" not in "\n".join(sent[1] for sent in telegram.sent)


def test_oversize_response_splits_losslessly_into_labeled_parts():
    # A response too large for one rich message still splits, losslessly, into
    # labeled "Response i/N" parts -- each under the per-message cap.
    tail = "TAIL_MARKER_LOSSLESS"
    text = "## **Long**\n\n" + ("- keep **rich** sections\n" * 1_100) + tail
    parts = render_feed_item_delivery_html_parts({"kind": "turn", "assistant_final_text": text})

    assert len(render_turn_item_html({"kind": "turn", "assistant_final_text": text})) > MAX_RICH_HTML_CHARS
    assert len(parts) > 1
    total = len(parts)
    for index, part in enumerate(parts, start=1):
        assert part.startswith(f"<b>✅ Response {index}/{total}</b><br><br>")
        assert len(part) <= MAX_RICH_HTML_CHARS
    combined = "\n".join(parts)
    assert tail in combined                       # nothing cut
    assert combined.count("<b>✅ Response ") == total  # one marker per part


def test_promote_to_final_uses_bounded_plain_sends_above_edit_cap(monkeypatch):
    # A final above the ordinary-message cap is split into readable plain parts.
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_MARKER_SENDSINGLE"
    body = "## **Long**\n\n" + ("- keep **rich** sections\n" * 180) + tail
    working_turns = {
        "turns": [
            {
                "id": "turn-midlong",
                "worker_id": "worker-1",
                "assistant_stream_text": "Working on it.",
                "complete": False,
            }
        ]
    }
    final_turns = {
        "turns": [
            {
                "id": "turn-midlong",
                "worker_id": "worker-1",
                "assistant_final_text": body,
                "complete": True,
            }
        ]
    }

    sync_once(store, SyncRuntime(FakeTendwire(turns=working_turns), telegram, with_outbox=False))
    result = sync_once(store, SyncRuntime(FakeTendwire(turns=final_turns), telegram, with_outbox=False))

    full_html_len = len(render_turn_item_html({"kind": "turn", "assistant_final_text": body}))
    assert full_html_len > 3900            # above legacy edit cap -> cannot edit
    assert full_html_len <= MAX_RICH_HTML_CHARS

    response_messages = [sent[1] for sent in telegram.sent if "✅ Response" in sent[1]]
    assert result["feed_sent"] == 1
    assert len(response_messages) > 1
    assert response_messages[0].startswith("✅ Response 1/")
    assert any(tail in message for message in response_messages)


def test_expandable_blockquote_has_delivery_fallbacks():
    variants = TelegramClient(token="fake", dry_run=True)._html_variants(
        "<b>Response</b>\n<blockquote expandable>hello <b>there</b></blockquote>"
    )

    assert variants[0][0] == "html"
    assert variants[1] == (
        "html-no-expandable",
        "<b>Response</b>\n<blockquote>hello <b>there</b></blockquote>",
    )
    assert variants[-1] == ("plain", "Response\nhello there")


def test_outbox_attention_falls_back_when_general_thread_missing():
    class OutboxTendwire:
        def __init__(self):
            self.acked = []
            self.failed = []

        def connector_poll(self, **_kwargs):
            return {
                "ok": True,
                "items": [
                    {
                        "ref": "ref-1",
                        "key": "attention:1",
                        "attempt": 1,
                        "payload": {
                            "event_type": "attention_created",
                            "attention": {"severity": "warning", "reason": "Needs input"},
                        },
                    }
                ],
            }

        def connector_ack(self, ref, response, **_kwargs):
            self.acked.append((ref, response))
            return {"ok": True}

        def connector_fail(self, ref, error, **_kwargs):
            self.failed.append((ref, error))
            return {"ok": True}

    class TopicMissingTelegram(FakeTelegram):
        def send_message(self, chat_id, html, **kwargs):
            if kwargs.get("thread_id"):
                return {"ok": False, "error": "Bad Request: message thread not found"}
            return super().send_message(chat_id, html, **kwargs)

    store = _store()
    stale_alias = {
        "source": "tendwire",
        "entry_type": "worker",
        "topic_id": "1",
        "topic_name": "stale general alias",
        "status": "closed",
    }
    store["panes"]["worker:stale-general-alias"] = stale_alias
    tendwire = OutboxTendwire()
    telegram = TopicMissingTelegram()

    result = drain_outbox(store, telegram, tendwire, chat_id="-100", max_sends=1)

    assert result["delivered"] == 1
    assert result["acked"] == 1
    assert result["failed"] == 0
    assert tendwire.failed == []
    assert tendwire.acked == [("ref-1", {"telegram": "delivered"})]
    assert telegram.sent[-1][2].get("thread_id") is None
    assert store["telegram_dead_topic_ids"] == ["1"]
    assert "topic_id" not in stale_alias
    assert stale_alias["deleted_topic_id"] == "1"


def test_long_telegram_send_splits_instead_of_truncating():
    class CapturingTelegram(TelegramClient):
        def __init__(self):
            super().__init__(token="fake")
            object.__setattr__(self, "payloads", [])

        def api(self, method, payload):
            self.payloads.append((method, payload))
            return {"ok": True, "result": {"message_id": len(self.payloads)}}

    telegram = CapturingTelegram()
    tail = "TAIL_MARKER_TELEGRAM_SPLIT"
    result = telegram.send_message("-100", "<b>Long</b>\n" + ("word " * 1200) + tail, thread_id="77")

    assert result["ok"] is True
    assert result["format"] == "plain-split"
    assert len(result["message_ids"]) > 1
    assert all(len(payload["text"]) <= 3900 for _method, payload in telegram.payloads)
    assert all("parse_mode" not in payload for _method, payload in telegram.payloads)
    assert any(tail in payload["text"] for _method, payload in telegram.payloads)


def test_existing_final_message_is_not_reposted_for_render_version_churn(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "## **Fixed**\n\n- now rich",
                "complete": True,
            }
        ]
    }
    runtime = SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False)

    assert sync_once(store, runtime)["feed_sent"] == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    entry["last_render_version"] = "old"
    sent_before = list(telegram.sent)
    telegram.edited.clear()

    assert sync_once(store, runtime)["feed_sent"] == 0
    assert telegram.sent == sent_before
    assert telegram.edited == []


def test_completed_turn_content_churn_is_not_reposted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    first_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "First final",
                "complete": True,
            }
        ]
    }
    second_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "First final with formatting changed",
                "complete": True,
            }
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    sent_before = list(telegram.sent)
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=second_turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert second["turn_updates"] == 1
    assert telegram.sent == sent_before
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-1"
    assert len([key for key in store["tendwire_source_delivered_turns"] if key.startswith("final:turn-1:")]) == 1


def test_delivered_final_turn_repairs_stale_entry_without_repost(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}), )
    worker["last_turn_id"] = "old-turn"
    worker["last_clean_message_id"] = "old-message"
    state.bind_message_to_worker(store, "555", worker, topic_id="77", kind="final", turn_id="turn-1", bot_kind="codex")
    state.mark_delivered(store, "final:turn-1:oldhash", {"worker_id": "worker-1", "turn_id": "turn-1"})
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Current final text",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["feed_sent"] == 0
    assert result["turn_updates"] == 1
    assert not any("Current final text" in sent[1] for sent in telegram.sent)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-1"
    assert entry["last_clean_message_id"] == "555"
    assert entry["last_clean_message_ids"] == ["555"]
    assert entry["last_clean_bot_kind"] == "codex"


def test_historical_same_worker_final_is_suppressed_without_churning_latest(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-new",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "New final",
                "complete": True,
            },
            {
                "id": "turn-old",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Old final",
                "complete": True,
            },
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert "New final" in "\n".join(sent[1] for sent in telegram.sent)
    assert "Old final" not in "\n".join(sent[1] for sent in telegram.sent)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-new"
    assert second["feed_sent"] == 0
    assert second["turn_updates"] == 0
    assert entry["last_turn_id"] == "turn-new"
    assert any(key.startswith("final:turn-old:") for key in store["tendwire_source_delivered_turns"])


def test_only_latest_working_turn_per_worker_is_delivered(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-new-open",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_stream_text": "new working text",
                "complete": False,
                "updated_at": "2026-07-03T16:24:15+00:00",
            },
            {
                "id": "turn-old-open",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_stream_text": "old working text",
                "complete": False,
            },
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))
    sent_text = "\n".join(sent[1] for sent in telegram.sent)
    sent_before = list(telegram.sent)
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert "new working text" in sent_text
    assert "old working text" not in sent_text
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_stream_turn_id"] == "turn-new-open"
    assert second["feed_sent"] == 0
    assert telegram.sent == sent_before


def test_delivered_turn_ledger_keeps_more_than_old_1000_limit():
    store = {}
    for index in range(1001):
        state.mark_delivered(store, f"final:turn-{index}:hash", {"turn_id": f"turn-{index}"})

    ledger = store["tendwire_source_delivered_turns"]
    assert len(ledger) == 1001
    assert "final:turn-0:hash" in ledger


def test_same_turn_working_edits_are_rate_limited(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "15")
    now = {"value": 1000.0}
    monkeypatch.setattr(source_sync.time, "time", lambda: now["value"])
    store = _store()
    telegram = FakeTelegram()
    turns = {"turns": [{"id": "turn-current", "worker_id": "worker-1", "assistant_stream_text": "step 1", "complete": False}]}
    tendwire = FakeTendwire(turns=turns)

    first = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    turns["turns"][0]["assistant_stream_text"] = "step 2"
    second = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    now["value"] = 1016.0
    third = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert third["feed_sent"] == 1
    working_sends = [sent for sent in telegram.sent if "step 1" in sent[1]]
    working_edits = [edited for edited in telegram.edited if "step 2" in edited[2]]
    assert len(working_sends) == 1
    assert len(working_edits) == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_stream_hash"] == source_sync._turn_content_hash(turns["turns"][0], "working")


def test_one_submission_keeps_one_working_card_across_five_rotating_passes(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "0"
    )
    store = _store()
    telegram = FakeTelegram()
    submission_id = "twsub1." + ("a" * 64)
    worker = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-1",
        }
    )
    _entry_key, entry, _created = state.upsert_worker_entry(
        store, worker, topic_id="77"
    )
    stable_identity = state.entry_stable_identity(entry)
    assert stable_identity is not None
    now = source_sync.time.time()
    record, _changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=now,
        retry_horizon=60,
        retention=3600,
    )
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": REQUEST_ID,
        "dry_run": False,
        "target": {
            "worker_id": "worker-1",
            "worker_fingerprint": "fp-1",
        },
        "instruction": {"text": "take the long action"},
    }
    ingress_requests.attach_request_json(
        record,
        ingress_requests.canonical_request_json(request),
        now=now,
    )
    ingress_requests.attach_target_owner(
        record, stable_identity[0], stable_identity[1], now=now
    )
    ingress_requests.attach_submission_receipt(
        record,
        submission_id,
        "pending_observation",
        None,
        now=now,
    )
    ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=now,
        reply="Sent to Tendwire worker.",
    )

    for index in range(5):
        turn = {
            "id": f"turn-pass-{index + 1}",
            "worker_id": "worker-1",
            "space_id": "space-1",
            "user_text": "take the long action",
            "assistant_stream_text": f"pass {index + 1} progress",
            "complete": False,
        }
        sync_once(
            store,
            SyncRuntime(
                FakeTendwire(
                    turns={"turns": [turn]},
                    workers=[worker],
                ),
                telegram,
                with_outbox=False,
            ),
        )

    working_sends = [
        sent for sent in telegram.sent if "Working" in sent[1]
    ]
    assert len(working_sends) == 1
    working_message_id = str(working_sends[0][3])
    assert len(telegram.edited) == 5
    assert {
        str(_message_id)
        for _chat, _message_id, _html in telegram.edited
    } == {working_message_id}
    assert "pass 5 progress" in telegram.edited[-1][2]
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_stream_submission_id"] == submission_id
    assert entry["last_stream_turn_id"] == "turn-pass-5"
    bindings = [
        binding
        for binding in state.message_bindings(store).values()
        if binding.get("kind") == "working"
    ]
    assert len(bindings) == 1
    assert bindings[0]["submission_id"] == submission_id
    assert bindings[0]["turn_id"] == "turn-pass-5"

    final = {
        "id": "turn-final-pass",
        "worker_id": "worker-1",
        "space_id": "space-1",
        "user_text": "take the long action",
        "assistant_final_text": "long action complete",
        "complete": True,
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                turns={"turns": [final]},
                workers=[worker],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert len(
        [sent for sent in telegram.sent if "Working" in sent[1]]
    ) == 1
    assert telegram.edited[-1][1] == working_message_id
    assert "long action complete" in telegram.edited[-1][2]
    assert record["submission_state"] == "complete"
    assert "last_stream_message_id" not in entry


def test_successive_turns_start_new_cards_and_omit_unattributed_stale_quote(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "0"
    )
    store = _store()
    telegram = FakeTelegram()
    first_submission = "twsub1." + ("b" * 64)
    second_submission = "twsub1." + ("c" * 64)

    first = {
        "id": "turn-first",
        "worker_id": "worker-1",
        "user_text": "first Telegram instruction",
        "assistant_stream_text": "first progress",
        "complete": False,
        "_herdres_submission_id": first_submission,
    }
    second = {
        "id": "turn-second",
        "worker_id": "worker-1",
        "user_text": "second Telegram instruction",
        "assistant_stream_text": "second progress",
        "complete": False,
        "_herdres_submission_id": second_submission,
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [first]}),
            telegram,
            with_outbox=False,
        ),
    )
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [second]}),
            telegram,
            with_outbox=False,
        ),
    )

    working_sends = [
        sent for sent in telegram.sent if "Working" in sent[1]
    ]
    assert len(working_sends) == 2
    assert working_sends[0][3] != working_sends[1][3]
    assert "first Telegram instruction" in working_sends[0][1]
    assert "second Telegram instruction" in working_sends[1][1]

    completed_second = {
        **second,
        "assistant_stream_text": None,
        "assistant_final_text": "second response",
        "complete": True,
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [completed_second]}),
            telegram,
            with_outbox=False,
        ),
    )
    assert telegram.edited[-1][1] == working_sends[1][3]

    internal = {
        "id": "turn-internal",
        "worker_id": "worker-1",
        # Tendwire repeated the prior Telegram text even though this turn was
        # opened by automation and carries no submission link.
        "user_text": "second Telegram instruction",
        "assistant_stream_text": "automation progress remains visible",
        "complete": False,
        "source": "worker:worker-1",
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [internal]}),
            telegram,
            with_outbox=False,
        ),
    )

    working_sends = [
        sent for sent in telegram.sent if "Working" in sent[1]
    ]
    assert len(working_sends) == 3
    internal_html = working_sends[-1][1]
    assert "automation progress remains visible" in internal_html
    assert "second Telegram instruction" not in internal_html
    assert "<b>You</b>" not in internal_html


def test_current_worker_final_without_updated_at_beats_older_command_turn(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-current-worker",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "source": "worker:worker-1",
                "assistant_final_text": "fresh current worker final",
                "complete": True,
            },
            {
                "id": "turn-old-command",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "source": "command",
                "assistant_final_text": "stale command final",
                "complete": True,
                "updated_at": "2026-07-03T16:21:55+00:00",
            },
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))
    sent_text = "\n".join(sent[1] for sent in telegram.sent)

    assert result["feed_sent"] == 1
    assert "fresh current worker final" in sent_text
    assert "stale command final" not in sent_text


def test_topic_icon_cache_is_fetched_and_working_icon_updates(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    {
                        "id": "worker-1",
                        "name": "Alpha",
                        "status": "working",
                        "space_id": "space-1",
                        "fingerprint": "fp-1",
                    }
                ],
                turns={"turns": []},
            ),
            telegram,
            with_outbox=False,
        ),
    )

    # Routine working status no longer flips a status icon; the topic gets its
    # stable identity icon once (the only non-reserved emoji in the fake set).
    assert result["icon_updated"] == 1
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]
    assert store["telegram"]["forum_topic_icons"]["by_emoji"]["⚡️"] == "icon-working"
    entry = next(iter(state.source_space_entries(store).values()))
    assert entry["last_topic_icon"] == "🦊"
    assert entry["last_topic_icon_id"] == "icon-fox"


def test_topic_icon_reapplies_when_local_emoji_state_lacks_icon_id(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    store["spaces"]["space:space-1:existing"] = {
        "source": "tendwire",
        "entry_type": "space",
        "tendwire_space_id": "space-1",
        "space_id": "space-1",
        "topic_name": "Project",
        "topic_id": "77",
        "last_topic_icon": "⚡️",
    }
    telegram = FakeTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(turns={"turns": []}), telegram, with_outbox=False))

    assert result["icon_updated"] == 1
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]
    entry = next(iter(state.source_space_entries(store).values()))
    assert entry["last_topic_icon"] == "🦊"
    assert entry["last_topic_icon_id"] == "icon-fox"


def test_topic_icon_not_modified_repairs_local_icon_state(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")

    class NotModifiedTelegram(FakeTelegram):
        def edit_topic_icon(self, chat_id, thread_id, emoji_id):
            self.icon_edits.append((str(chat_id), str(thread_id), str(emoji_id)))
            return {"ok": False, "error": "Bad Request: TOPIC_NOT_MODIFIED"}

    store = _store()
    telegram = NotModifiedTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(turns={"turns": []}), telegram, with_outbox=False))

    assert result["icon_updated"] == 1
    entry = next(iter(state.source_space_entries(store).values()))
    assert entry["last_topic_icon"] == "🦊"
    assert entry["last_topic_icon_id"] == "icon-fox"
    assert "last_topic_icon_error" not in entry


def test_active_source_status_with_completed_turn_uses_idle_topic_icon(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-done",
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "assistant_final_text": "done",
                    "complete": True,
                }
            ]
        },
        workers=[
            {
                "id": "worker-1",
                "name": "codex",
                "status": "active",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    result = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    entry = next(iter(state.source_space_entries(store).values()))

    assert result["icon_updated"] == 1
    assert entry["status"] == "idle"
    assert entry["active_worker_status"] == "idle"
    assert entry["last_topic_icon"] == "🦊"
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]


def test_open_turn_from_retired_worker_id_does_not_pin_topic_icon(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-open",
                    "worker_id": "worker-old",
                    "space_id": "space-1",
                    "assistant_stream_text": "working",
                    "complete": False,
                    "has_open_turn": True,
                }
            ]
        },
        workers=[
            {
                "id": "worker-new",
                "name": "codex",
                "status": "active",
                "space_id": "space-1",
                "fingerprint": "fp-new",
            }
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    result = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    entry = next(iter(state.source_space_entries(store).values()))

    # Worker ids are stable now; a lingering open turn from a retired worker id
    # must not pin the live space to "working" forever.
    assert result["icon_updated"] == 1
    assert entry["status"] == "idle"
    assert entry["active_worker_status"] == "idle"
    assert entry["last_topic_icon"] == "🦊"
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]


def test_public_raw_status_working_overrides_done_source_status(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={"turns": []},
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "done",
                "space_id": "space-1",
                "fingerprint": "fp-1",
                "meta": {"agent": "claude", "raw_status": "working"},
            }
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    result = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    entry = next(iter(state.source_space_entries(store).values()))

    assert result["icon_updated"] == 1
    assert result["feed_sent"] == 0
    assert entry["status"] == "working"
    assert entry["active_worker_status"] == "working"
    assert entry["last_topic_icon"] == "🦊"


def test_empty_current_turn_for_working_worker_sends_compact_working_update(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-current",
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "status": "done",
                }
            ]
        },
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "done",
                "space_id": "space-1",
                "fingerprint": "fp-1",
                "meta": {"agent": "claude", "raw_status": "working"},
            }
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    first = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    second = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    entry = next(iter(state.source_worker_entries(store).values()))

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert entry["last_stream_turn_id"] == "turn-current"
    assert any("Work is in progress." in sent[1] for sent in telegram.sent)


def test_space_topic_pin_renders_worker_board_not_space_summary(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_worker_entry(store, _source_worker({
        "id": "worker-stale-claude",
        "name": "claude",
        "status": "idle",
        "space_id": "space-1",
        "fingerprint": "fp-stale",
        "meta": {"agent": "claude"},
    }), )
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={"turns": []},
        workers=[
            {
                "id": "worker-claude",
                "name": "claude",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-claude",
                "meta": {"agent": "claude", "model": "claude-opus-4-8"},
            },
            {
                "id": "worker-codex",
                "name": "codex",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "fp-codex",
                "model": "gpt-5-codex",
                "meta": {"agent": "codex"},
            },
        ],
    )

    result = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    topic_status_html = "\n".join(sent[1] for sent in telegram.sent if sent[2].get("thread_id") == "77")

    assert result["pinned_status_updated"] >= 1
    assert "Codex · GPT-5 Codex 🟡" in topic_status_html
    assert "Claude · Opus 4.8 🟢" in topic_status_html
    assert topic_status_html.count("Claude") == 1
    assert "active:" not in topic_status_html
    assert "Project" not in topic_status_html


def test_topic_pinned_status_reuses_legacy_topic_pin(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    store["spaces"]["workspace:space-1"] = {
        "topic_name": "Project",
        "topic_id": "77",
        "pinned_status_message_id": "55",
    }
    telegram = FakeTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(turns={"turns": []}), telegram, with_outbox=False))
    entry = next(iter(state.source_space_entries(store).values()))

    assert result["pinned_status_updated"] >= 1
    assert entry["pinned_status_message_id"] == "55"
    assert entry["pinned_status_pinned"] is True
    assert ("-100", "55") in telegram.pins
    assert any(edit[1] == "55" for edit in telegram.edited)
    assert not any(sent[2].get("thread_id") == "77" for sent in telegram.sent)


def _notification_race_store(*, retired=False):
    store = _store()
    store["panes"]["worker:notification-race"] = {
        "source": "tendwire",
        "entry_type": "worker",
        "tendwire_worker_id": "worker-notification",
        "worker_id": "worker-notification",
        "tendwire_space_id": "space-1",
        "space_id": "space-1",
        "tendwire_fingerprint": "fp-notification",
        "topic_id": "77",
        "topic_name": "Notification race",
        "status": "retired" if retired else "idle",
        "tendwire_raw_status": "idle",
    }
    if retired:
        store["panes"]["worker:notification-race"].update(
            {
                "routing_retired": True,
                "retired_topic_notice_pending": True,
            }
        )
    return store


def _ten_notification_entries(*, retired: bool = False):
    store = _notification_race_store(retired=retired)
    store["panes"] = {}
    for index in range(10):
        key = f"worker:notification-{index}"
        entry = {
            "source": "tendwire",
            "entry_type": "worker",
            "tendwire_worker_id": f"worker-notification-{index}",
            "worker_id": f"worker-notification-{index}",
            "tendwire_space_id": "space-1",
            "space_id": "space-1",
            "tendwire_fingerprint": f"fp-notification-{index}",
            "topic_id": str(770 + index),
            "topic_name": f"Notification {index}",
            "status": "retired" if retired else "idle",
            "tendwire_raw_status": "idle",
        }
        if retired:
            entry.update(
                {
                    "routing_retired": True,
                    "retired_topic_notice_pending": True,
                }
            )
        store["panes"][key] = entry
    return store


def _notification_save_counter(
    tmp_path, monkeypatch, store, delivery
) -> int:
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    state.save_state(store, state_path)
    real_save_state = state.save_state
    save_calls = 0

    def counted_save(current, path=None):
        nonlocal save_calls
        save_calls += 1
        return real_save_state(current, path=path)

    monkeypatch.setattr(state, "save_state", counted_save)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                FakeTelegram(),
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, state_path
                ),
            ),
        )
        delivery(current, runtime)
    return save_calls


def test_ten_topic_pin_deliveries_use_two_durability_barriers_each(
    tmp_path, monkeypatch
):
    def deliver(current, runtime):
        for key in list(current["panes"]):
            entry = current["panes"][key]
            assert source_sync._sync_topic_pinned(
                current, entry, runtime, chat_id="-100"
            )

    assert (
        _notification_save_counter(
            tmp_path,
            monkeypatch,
            _ten_notification_entries(),
            deliver,
        )
        == 20
    )


def test_ten_retired_notices_use_two_durability_barriers_each(
    tmp_path, monkeypatch
):
    def deliver(current, runtime):
        assert (
            source_sync._sync_retired_worker_topics(
                current, runtime, chat_id="-100"
            )
            == 10
        )

    assert (
        _notification_save_counter(
            tmp_path,
            monkeypatch,
            _ten_notification_entries(retired=True),
            deliver,
        )
        == 20
    )


def test_ten_global_status_deliveries_use_two_durability_barriers_each(
    tmp_path, monkeypatch
):
    def deliver(current, runtime):
        for _index in range(10):
            assert source_sync._sync_pinned(
                current, runtime, chat_id="-100"
            )
            telegram = current.setdefault("telegram", {})
            telegram.pop("pinned_status_message_id", None)
            telegram.pop("pinned_status_hash", None)

    assert (
        _notification_save_counter(
            tmp_path,
            monkeypatch,
            _ten_notification_entries(),
            deliver,
        )
        == 20
    )


@pytest.mark.parametrize(
    "kind",
    ["topic_pinned", "retired_topic_notice", "global_pinned"],
)
def test_notification_acceptance_journal_closes_crash_seam(
    tmp_path, monkeypatch, kind
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    initial = _notification_race_store(
        retired=kind == "retired_topic_notice"
    )
    state.save_state(initial, state_path)
    telegram = CrashNotificationTelegram()

    def crash_after_accept():
        raise RuntimeError("crash after notification acceptance")

    def deliver(current, runtime):
        if kind == "topic_pinned":
            entry = current["panes"]["worker:notification-race"]
            return source_sync._sync_topic_pinned(
                current, entry, runtime, chat_id="-100"
            )
        if kind == "retired_topic_notice":
            return bool(
                source_sync._sync_retired_worker_topics(
                    current, runtime, chat_id="-100"
                )
            )
        return source_sync._sync_pinned(
            current, runtime, chat_id="-100"
        )

    with pytest.raises(
        RuntimeError, match="crash after notification acceptance"
    ):
        with state.state_lock(state_path):
            current = state.load_state(state_path)
            runtime = source_sync._offlock_runtime(
                current,
                SyncRuntime(
                    FakeTendwire(),
                    telegram,
                    with_outbox=False,
                    checkpoint=lambda: state.save_state(
                        current, state_path
                    ),
                    after_provider_accept=crash_after_accept,
                ),
            )
            deliver(current, runtime)

    # The 11.7 MB ledger has not crossed another barrier, but the small
    # acceptance fact is already durable in the sidecar.
    durable_before_restart = json.loads(
        state_path.read_text(encoding="utf-8")
    )
    assert not durable_before_restart["telegram"].get(
        "accepted_notification_messages"
    )
    assert state.accepted_notification_journal_path(
        state_path
    ).exists()

    with state.state_lock(state_path):
        current = state.load_state(state_path)
        assert len(
            current["telegram"]["accepted_notification_messages"]
        ) == 1
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, state_path
                ),
            ),
        )
        retired, pending = source_sync._drain_accepted_notifications(
            current, runtime, chat_id="-100"
        )
        assert deliver(current, runtime)
        state.save_state(current, state_path)

    deleted_ids = {message_id for _chat, message_id in telegram.deleted_messages}
    visible_ids = {
        str(row[3]) for row in telegram.sent
    } - deleted_ids
    assert (retired, pending) == (1, 0)
    assert visible_ids == {"101"}
    assert not state.accepted_notification_journal_path(
        state_path
    ).exists()
    assert not state.load_state(state_path)["telegram"].get(
        "accepted_notification_messages"
    )


def test_topic_pin_rebind_checkpoints_and_retires_accepted_card(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    state.save_state(_notification_race_store(), state_path)

    def rebind(current):
        current["panes"]["worker:notification-race"]["topic_id"] = "88"

    telegram = RebindingNotificationTelegram(state_path, rebind)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
        )
        entry = current["panes"]["worker:notification-race"]
        assert (
            source_sync._sync_topic_pinned(
                current, entry, runtime, chat_id="-100"
            )
            is False
        )
        assert len(
            current["telegram"]["accepted_notification_messages"]
        ) == 1
        retired, pending = source_sync._drain_accepted_notifications(
            current, runtime, chat_id="-100"
        )
        entry = current["panes"]["worker:notification-race"]
        assert source_sync._sync_topic_pinned(
            current, entry, runtime, chat_id="-100"
        )

    assert (retired, pending) == (1, 0)
    assert telegram.deleted_messages == [("-100", "100")]
    assert [row[2]["thread_id"] for row in telegram.sent] == [
        "77",
        "88",
    ]
    assert current["panes"]["worker:notification-race"][
        "pinned_status_message_id"
    ] == "101"


def test_retired_notice_rebind_checkpoints_and_retires_accepted_card(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    state.save_state(
        _notification_race_store(retired=True), state_path
    )

    def rebind(current):
        current["panes"]["worker:notification-race"]["topic_id"] = "88"

    telegram = RebindingNotificationTelegram(state_path, rebind)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
        )
        assert (
            source_sync._sync_retired_worker_topics(
                current, runtime, chat_id="-100"
            )
            == 0
        )
        retired, pending = source_sync._drain_accepted_notifications(
            current, runtime, chat_id="-100"
        )
        assert (
            source_sync._sync_retired_worker_topics(
                current, runtime, chat_id="-100"
            )
            == 1
        )

    assert (retired, pending) == (1, 0)
    assert telegram.deleted_messages == [("-100", "100")]
    assert [row[2]["thread_id"] for row in telegram.sent] == [
        "77",
        "88",
    ]
    entry = current["panes"]["worker:notification-race"]
    assert entry["retired_topic_notice_message_id"] == "101"
    assert "retired_topic_notice_pending" not in entry


def test_global_pin_rebind_checkpoints_and_retires_accepted_card(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    state.save_state(_notification_race_store(), state_path)

    def rebind(current):
        current["telegram"]["general_thread_id"] = "2"

    telegram = RebindingNotificationTelegram(state_path, rebind)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
        )
        assert (
            source_sync._sync_pinned(
                current, runtime, chat_id="-100"
            )
            is False
        )
        retired, pending = source_sync._drain_accepted_notifications(
            current, runtime, chat_id="-100"
        )
        assert source_sync._sync_pinned(
            current, runtime, chat_id="-100"
        )

    assert (retired, pending) == (1, 0)
    assert telegram.deleted_messages == [("-100", "100")]
    assert [row[2]["thread_id"] for row in telegram.sent] == ["1", "2"]
    assert current["telegram"]["pinned_status_message_id"] == "101"


def test_topic_pinned_edit_topic_not_found_resends_before_tombstoning(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")

    class MissingPinnedEditTelegram(FakeTelegram):
        def edit_message(self, chat_id, message_id, html):
            if str(message_id) == "55":
                return {
                    "ok": False,
                    "kind": "topic_not_found",
                    "error": "Bad Request: message thread not found",
                }
            return super().edit_message(chat_id, message_id, html)

    store = _store()
    store["spaces"]["workspace:space-1"] = {
        "topic_name": "Project",
        "topic_id": "77",
        "pinned_status_message_id": "55",
    }
    telegram = MissingPinnedEditTelegram()

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": []}),
            telegram,
            with_outbox=False,
        ),
    )
    entry = next(iter(state.source_space_entries(store).values()))

    assert entry["topic_id"] == "77"
    assert store.get("telegram_dead_topic_ids", []) == []
    assert entry["pinned_status_message_id"] != "55"
    assert any(
        sent[2].get("thread_id") == "77"
        for sent in telegram.sent
    )


def test_pinned_status_falls_back_when_general_thread_is_missing(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")

    class MissingGeneralThreadTelegram(FakeTelegram):
        def send_message(self, chat_id, html, **kwargs):
            if str(kwargs.get("thread_id") or "") == "1":
                return {"ok": False, "error": "Bad Request: message thread not found"}
            return super().send_message(chat_id, html, **kwargs)

    store = _store()
    telegram = MissingGeneralThreadTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(turns={"turns": []}), telegram, with_outbox=False))

    assert result["pinned_status_updated"] >= 1
    assert telegram.pins
    assert store["telegram"]["pinned_status_message_id"]
    assert "pinned_status_last_error" not in store["telegram"]


def test_working_update_edits_existing_message(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "0")
    store = _store()
    telegram = FakeTelegram()
    first_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "first", "complete": False}]}
    second_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "second", "complete": False}]}

    sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    sync_once(store, SyncRuntime(FakeTendwire(turns=second_turns), telegram, with_outbox=False))

    assert len(telegram.sent) >= 1
    assert telegram.edited
    assert "second" in telegram.edited[-1][2]


def test_recovered_final_promotes_existing_working_card_once_without_replay(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store = _store()
    store["telegram"]["account_baseline"] = {
        "codex": "ChatGPT Pro",
        "five_hour_remaining": 82,
    }
    worker_observation = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "idle",
            "space_id": "space-1",
            "fingerprint": "fp-1",
            "meta": {"agent": "codex"},
        }
    )
    _worker_key, worker, _created = state.upsert_worker_entry(
        store, worker_observation, topic_id="77"
    )
    _space_key, space, _created = state.upsert_space_entry(
        store,
        {
            "id": "space-1",
            "name": "Project",
            "status": "active",
            "fingerprint": "space-fp-1",
        },
        topic_id="77",
    )
    space.update(
        {
            "pinned_status_message_id": "55",
            "pinned_status_hash": "pin-hash",
            "pinned_status_pinned": True,
            "last_topic_icon": "🦊",
            "last_topic_icon_id": "icon-fox",
        }
    )
    worker.update(
        {
            "last_stream_turn_id": "turn-recovered",
            "last_stream_hash": "working-hash",
            "last_stream_message_id": "555",
            "last_stream_bot_kind": "manager",
        }
    )
    state.bind_message_to_worker(
        store,
        "555",
        worker,
        topic_id="77",
        kind="working",
        turn_id="turn-recovered",
        bot_kind="manager",
    )
    preserved = {
        "space_topic": {
            key: space[key]
            for key in (
                "topic_id",
                "topic_name",
                "pinned_status_message_id",
                "pinned_status_hash",
                "pinned_status_pinned",
                "last_topic_icon",
                "last_topic_icon_id",
            )
        },
        "worker_topic_id": worker["topic_id"],
        "account": dict(store["telegram"]["account_baseline"]),
    }
    final_payload = {
        "schema_version": 1,
        "turns": [
            {
                "id": "turn-recovered",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "space_id": "space-1",
                "user_text": "Recover the missed response",
                "assistant_final_text": "Recovered authoritative final",
                "complete": True,
            }
        ],
    }

    class RecoveryTendwire(FakeTendwire):
        def __init__(self):
            super().__init__(
                turns=final_payload,
                workers=[worker_observation],
                spaces=[
                    {
                        "id": "space-1",
                        "name": "Project",
                        "status": "active",
                        "fingerprint": "space-fp-1",
                    }
                ],
            )
            self.calls = []

        def snapshot(self):
            self.calls.append("snapshot")
            return super().snapshot()

        def turns(self):
            self.calls.append("turns")
            return super().turns()

        def pending(self):
            self.calls.append("pending")
            return super().pending()

    tendwire = RecoveryTendwire()
    telegram = FakeTelegram()
    runtime = SyncRuntime(tendwire, telegram, with_outbox=False)

    direct_boundary_attempts = []

    def reject_direct_boundary(*_args, **_kwargs):
        direct_boundary_attempts.append("process_or_socket")
        raise AssertionError("source sync must not access Herdr outside Tendwire")

    real_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        private_roots = {
            "herdr",
            "herdr_turn_adapter",
            "herdr_socket",
            "herdr_cli",
            "herdr_events",
        }
        if name.split(".", 1)[0] in private_roots or name.startswith(
            "tendwire.backends"
        ):
            direct_boundary_attempts.append(f"import:{name[:80]}")
            raise AssertionError("source sync must not import a direct Herdr client")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(subprocess, "run", reject_direct_boundary)
    monkeypatch.setattr(subprocess, "Popen", reject_direct_boundary)
    monkeypatch.setattr(socket, "socket", reject_direct_boundary)
    monkeypatch.setattr(socket, "create_connection", reject_direct_boundary)

    first = sync_once(store, runtime)
    ledger_after_first = json.loads(
        json.dumps(state.delivered_turns(store), sort_keys=True)
    )
    edits_after_first = list(telegram.edited)
    sends_after_first = list(telegram.sent)
    second = sync_once(store, runtime)
    ledger_after_second = json.loads(
        json.dumps(state.delivered_turns(store), sort_keys=True)
    )
    third = sync_once(store, runtime)

    entry = next(iter(state.source_worker_entries(store).values()))
    binding = state.find_message_binding(store, "555", topic_id="77")
    ledger = state.delivered_turns(store)
    assert first["feed_sent"] == 1
    assert first["sent"] == 1
    assert len(telegram.edited) == 1
    assert telegram.edited[0][1] == "555"
    assert "Recovered authoritative final" in telegram.edited[0][2]
    assert telegram.sent == []
    assert second["feed_sent"] == second["sent"] == second["turn_updates"] == 0
    assert third["feed_sent"] == third["sent"] == third["turn_updates"] == 0
    assert len(telegram.edited) == 1
    assert telegram.edited == edits_after_first
    assert telegram.sent == sends_after_first
    assert ledger_after_second == ledger_after_first
    assert ledger == ledger_after_first
    assert len(ledger) == 1
    assert list(ledger.values())[0]["turn_id"] == "turn-recovered"
    assert binding is not None
    assert binding["kind"] == "final"
    assert binding["turn_id"] == "turn-recovered"
    assert entry["last_turn_id"] == "turn-recovered"
    assert entry["last_clean_message_id"] == "555"
    assert entry["last_clean_message_ids"] == ["555"]
    assert "last_stream_turn_id" not in entry
    assert "last_stream_hash" not in entry
    assert "last_stream_message_id" not in entry
    assert "last_stream_bot_kind" not in entry
    assert {
        key: space[key]
        for key in (
            "topic_id",
            "topic_name",
            "pinned_status_message_id",
            "pinned_status_hash",
            "pinned_status_pinned",
            "last_topic_icon",
            "last_topic_icon_id",
        )
    } == preserved["space_topic"]
    assert entry["topic_id"] == preserved["worker_topic_id"]
    assert store["telegram"]["account_baseline"] == preserved["account"]
    assert telegram.pins == []
    assert telegram.icon_edits == []
    assert tendwire.calls == ["snapshot", "turns", "pending"] * 3
    assert tendwire.commands == []
    assert direct_boundary_attempts == []


def test_completed_turn_promotes_working_message_to_final(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    first_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "user_text": "Question",
                "assistant_stream_text": "still thinking",
                "complete": False,
            }
        ]
    }
    final_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "user_text": "Question",
                "assistant_final_text": "Final answer",
                "complete": True,
            }
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    entry = next(iter(state.source_worker_entries(store).values()))
    working_message_id = entry["last_stream_message_id"]
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=final_turns), telegram, with_outbox=False))
    binding = state.find_message_binding(store, working_message_id, topic_id="77")

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 1
    assert any(edit[1] == working_message_id and "Final answer" in edit[2] for edit in telegram.edited)
    assert not any("Final answer" in sent[1] for sent in telegram.sent)
    assert binding is not None
    assert binding["kind"] == "final"
    assert entry["last_clean_message_id"] == working_message_id
    assert entry["last_clean_message_ids"] == [working_message_id]
    assert "last_stream_message_id" not in entry


def test_completed_turn_sends_new_final_when_working_bot_kind_differs(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    telegram = FakeTelegram()
    first_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "working", "complete": False}]}
    final_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_final_text": "Bot-specific final", "complete": True}]}

    sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    entry = next(iter(state.source_worker_entries(store).values()))
    entry["last_stream_bot_kind"] = "claude"
    sync_once(store, SyncRuntime(FakeTendwire(turns=final_turns), telegram, with_outbox=False))

    assert not any("Bot-specific final" in edit[2] for edit in telegram.edited)
    assert any("Bot-specific final" in sent[1] for sent in telegram.sent)
    assert entry["last_clean_message_id"] != entry.get("last_stream_message_id")


def test_completed_long_turn_sends_final_without_cutting_content(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_PROMOTION_FALLBACK_67890"
    long_final = "## Long\n\n" + "- keep the complete response\n" * 260 + tail
    first_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "working", "complete": False}]}
    final_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_final_text": long_final, "complete": True}]}

    sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    sync_once(store, SyncRuntime(FakeTendwire(turns=final_turns), telegram, with_outbox=False))

    assert not any(tail in edit[2] for edit in telegram.edited)
    assert any(tail in sent[1] for sent in telegram.sent)


def test_open_working_turn_repairs_stale_final_markers_then_finalizes(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    state.mark_delivered(store, "final:turn-1:oldstream", {"worker_id": "worker-1", "turn_id": "turn-1"})
    state.bind_message_to_worker(store, "555", worker, topic_id="77", kind="final", turn_id="turn-1", bot_kind="codex")
    worker["last_turn_id"] = "turn-1"
    worker["last_clean_hash"] = "oldstream"
    worker["last_clean_message_id"] = "555"
    worker["last_clean_message_ids"] = ["555"]
    telegram = FakeTelegram()

    first = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "still working", "complete": False}]}),
            telegram,
            with_outbox=False,
        ),
    )
    entry = next(iter(state.source_worker_entries(store).values()))

    assert first["feed_sent"] == 1
    assert not any(key.startswith("final:turn-1:") for key in store["tendwire_source_delivered_turns"])
    assert state.find_message_binding(store, "555", topic_id="77") is None
    assert "last_clean_message_id" not in entry

    second = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_final_text": "real final", "complete": True}]}),
            telegram,
            with_outbox=False,
        ),
    )

    assert second["feed_sent"] == 1
    assert any("real final" in sent[1] for sent in telegram.sent) or any("real final" in edit[2] for edit in telegram.edited)
    assert any(key.startswith("final:turn-1:") for key in store["tendwire_source_delivered_turns"])


def test_command_reply_preserves_prederived_request_id_and_strips_private_ingress(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-1")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "12345",
            "request_id": REQUEST_ID,
            "reply_to_message_id": "12344",
            "update_id": "raw-update-sentinel",
            "user_id": "raw-user-sentinel",
            "bot_token": "raw-bot-sentinel",
            "backend_target": "private-route-sentinel",
            "text": "/send hello",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-1", "worker_fingerprint": "fp-1"}
    assert request["request_id"] == REQUEST_ID
    assert set(request) == {
        "schema_version",
        "action",
        "request_id",
        "dry_run",
        "target",
        "instruction",
    }
    encoded = json.dumps(request, sort_keys=True)
    assert all(
        forbidden not in encoded
        for forbidden in (
            "12345",
            "12344",
            "-100",
            "topic_id",
            "raw-update-sentinel",
            "raw-user-sentinel",
            "raw-bot-sentinel",
            "private-route-sentinel",
        )
    )



def test_stale_target_no_receipt_refresh_reuses_same_request_id(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-stale",
            }
        ),
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-1")[1]
    )
    state.save_state(store)
    calls = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            calls.append(request)
            if len(calls) == 1:
                return _failed_command_response(
                    request, status="stale_target", disposition="no_receipt"
                )
            return _accepted_command_response(request)

    monkeypatch.setattr(herdres, "TendwireClient", Client)

    result = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "12345",
            "text": "/send same intention",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    assert len(calls) == 2
    assert calls[0]["request_id"] == calls[1]["request_id"] == REQUEST_ID
    assert calls[0]["target"] == {
        "worker_id": "worker-1",
        "worker_fingerprint": "fp-stale",
    }
    assert calls[1]["target"] == {"worker_id": "worker-1"}
    assert {key: value for key, value in calls[0].items() if key != "target"} == {
        key: value for key, value in calls[1].items() if key != "target"
    }


def test_no_receipt_redelivery_reuses_durable_exact_request_across_state_churn(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-original",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)
    request_bytes = []

    class Client:
        def command_json(self, request_json):
            request_bytes.append(request_json)
            request = json.loads(request_json)
            if len(request_bytes) == 1:
                return _failed_command_response(
                    request,
                    status="backend_unavailable",
                    disposition="no_receipt",
                )
            return _accepted_command_response(request)

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    first = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "topic_id": "77",
            "message_id": "9001",
            "text": "/send original instruction",
        }
    )
    changed = state.load_state()
    changed["panes"] = {}
    state.save_state(changed)

    replay = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "topic_id": "route-is-now-gone",
            "message_id": "changed-private-coordinate",
            "text": "/send a retranscription must not replace the request",
        }
    )

    assert first == _gateway_child(
        REQUEST_ID,
        checkpoint=herdres_gateway.CHECKPOINT_RETRY,
        disposition="no_receipt",
    )
    assert replay == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    assert len(request_bytes) == 2
    assert request_bytes[0] == request_bytes[1]
    request = json.loads(request_bytes[0])
    assert request["instruction"] == {"text": "original instruction"}
    assert request["target"] == {
        "worker_id": "worker-1",
        "worker_fingerprint": "fp-original",
    }






def test_save_state_fsyncs_file_before_replace_and_directory_after(
    tmp_path,
    monkeypatch,
):
    calls = []
    real_fsync = os.fsync
    real_replace = os.replace

    def tracking_fsync(descriptor):
        kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
        calls.append(f"fsync:{kind}")
        real_fsync(descriptor)

    def tracking_replace(source, destination):
        calls.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr(state.os, "fsync", tracking_fsync)
    monkeypatch.setattr(state.os, "replace", tracking_replace)
    state_path = tmp_path / "nested" / "state.json"

    state.save_state({"version": 2, "value": "durable"}, state_path)

    assert calls == ["fsync:file", "replace", "fsync:directory"]
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "value": "durable",
        "version": 2,
    }


def test_save_state_existing_file_keeps_directory_fsync(
    tmp_path,
    monkeypatch,
):
    state_path = tmp_path / "state.json"
    state.save_state({"version": 2, "value": "before"}, state_path)
    calls = []
    real_fsync = os.fsync
    real_replace = os.replace

    def tracking_fsync(descriptor):
        kind = (
            "directory"
            if stat.S_ISDIR(os.fstat(descriptor).st_mode)
            else "file"
        )
        calls.append(f"fsync:{kind}")
        real_fsync(descriptor)

    def tracking_replace(source, destination):
        calls.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr(state.os, "fsync", tracking_fsync)
    monkeypatch.setattr(state.os, "replace", tracking_replace)

    state.save_state(
        {"version": 2, "value": "after"}, state_path
    )

    assert calls == ["fsync:file", "replace", "fsync:directory"]
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "value": "after",
        "version": 2,
    }


def test_save_state_sidecar_clear_keeps_directory_fsync(
    tmp_path,
    monkeypatch,
):
    state_path = tmp_path / "state.json"
    current = {"version": 2, "telegram": {}}
    state.save_state(current, state_path)
    record = {
        "kind": "topic_pinned",
        "topic_id": "77",
        "message_id": "101",
        "bot_kind": "manager",
        "provenance": {},
    }
    state.append_accepted_notification_receipt(
        "receipt-1",
        record,
        data=current,
        path=state_path,
    )
    calls = []
    real_fsync = os.fsync
    real_replace = os.replace
    real_unlink = Path.unlink
    journal_path = state.accepted_notification_journal_path(
        state_path
    )

    def tracking_fsync(descriptor):
        kind = (
            "directory"
            if stat.S_ISDIR(os.fstat(descriptor).st_mode)
            else "file"
        )
        calls.append(f"fsync:{kind}")
        real_fsync(descriptor)

    def tracking_replace(source, destination):
        calls.append("replace")
        real_replace(source, destination)

    def tracking_unlink(path, *args, **kwargs):
        if path == journal_path:
            calls.append("unlink:journal")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(state.os, "fsync", tracking_fsync)
    monkeypatch.setattr(state.os, "replace", tracking_replace)
    monkeypatch.setattr(Path, "unlink", tracking_unlink)

    state.save_state(current, state_path)

    assert calls == [
        "fsync:file",
        "replace",
        "fsync:directory",
        "unlink:journal",
        "fsync:directory",
    ]
    assert not state.accepted_notification_journal_path(
        state_path
    ).exists()


def test_legacy_duplicate_instruction_is_truthful_non_success():
    response = {
        "ok": True,
        "status": "duplicate_instruction",
        "result": {"delivery_state": "duplicate_suppressed"},
    }

    assert herdres._success_reply(response) == ""
    mislabeled = {
        "ok": True,
        "status": "accepted",
        "result": {"delivery_state": "duplicate_suppressed"},
    }
    assert herdres._success_reply(mislabeled) == ""


def test_invalid_snapshot_clears_stale_space_route_and_blocks_unbound_send_and_reply(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDR_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json")
    )
    store = _store()
    valid_worker = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "idle",
            "space_id": "space-1",
            "fingerprint": "fp-valid",
        }
    )
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[valid_worker], turns={"turns": []}),
            FakeTelegram(),
            with_outbox=False,
        ),
    )
    space = next(iter(state.source_space_entries(store).values()))
    worker_key, worker = state.find_worker_entry_by_id(store, "worker-1")
    assert worker_key is not None
    assert worker is not None
    topic_id = str(space["topic_id"])
    state.bind_message_to_worker(
        store, "500", worker, topic_id=topic_id, kind="final"
    )
    monkeypatch.setattr(
        source_sync,
        "_cleanup_topics",
        lambda *_args, **_kwargs: {
            "deleted": 0,
            "failed": 0,
            "pruned": 0,
            "changed": False,
        },
    )

    invalid_worker = {
        "id": "worker-1",
        "name": "Alpha",
        "status": "idle",
        "space_id": "space-1",
        "fingerprint": "fp-invalid",
        "meta": {"agent": "codex"},
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[invalid_worker],
                turns={"turns": []},
                stable_identities=False,
            ),
            FakeTelegram(),
            with_outbox=False,
        ),
    )

    space = next(iter(state.source_space_entries(store).values()))
    assert space["stale_space_topic"] is True
    assert not any(key.startswith("active_worker_") for key in space)
    assert state.find_entry_by_thread(store, topic_id) == (None, None)
    assert state.message_bindings(store)["500"]["routing_quarantined"] is True
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    assert (
        herdres_gateway._payload_for_message(
            {
                "chat": {"id": "-100", "is_forum": True},
                "message_thread_id": int(topic_id),
                "message_id": 600,
                "from": {"id": "1", "is_bot": False},
                "text": "unbound gateway send",
            },
            store,
        )
        is None
    )
    unbound = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": topic_id,
            "message_id": "601",
            "text": "/send unbound command",
        }
    )
    reply = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": topic_id,
            "message_id": "602",
            "reply_to_message_id": "500",
            "text": "/send stale reply",
        }
    )

    assert unbound == reply == {"handled": False}
    assert fake.commands == []


def test_space_route_requires_cached_exact_v1_identity(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
    )
    _space_key, space, _created = state.upsert_space_entry(
        store,
        {
            "id": "space-1",
            "name": "Project",
            "status": "active",
            "fingerprint": "space-fp",
        },
        topic_id="77",
    )

    assert state.find_entry_by_thread(store, "77")[1] is space
    space.pop("active_worker_stable_key_version")
    assert state.find_entry_by_thread(store, "77") == (None, None)
    assert state.cache_space_active_worker(space, worker) is True
    space["active_worker_stable_key"] = "wsk1_" + "f" * 64
    assert state.find_entry_by_thread(store, "77") == (None, None)
    assert state.cache_space_active_worker(space, worker) is True
    assert state.find_entry_by_thread(store, "77")[1] is space


def _stable_reply_worker(store):
    return state.upsert_worker_entry(store, _source_worker({
        "id": "worker-claude",
        "name": "claude",
        "status": "idle",
        "space_id": "space-1",
        "fingerprint": "fp-claude",
        "meta": {
            "agent": "claude",
            "stable_key": "wsk1_" + "a" * 64,
            "stable_key_version": 1,
        },
    }), topic_id="26",)


def test_stable_reply_binding_requires_resolved_worker_topic_ownership():
    store = _store()
    worker_key, worker, _created = _stable_reply_worker(store)
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )

    state.bind_message_to_worker(store, "500", worker, topic_id="99", kind="final")
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "99"},
    ) == (None, None)

    for message_id, topic_id in (("501", "26"), ("502", "77")):
        state.bind_message_to_worker(store, message_id, worker, topic_id=topic_id, kind="final")
        resolved_key, resolved_worker = herdres._worker_entry_from_reply(
            store,
            {"reply_to_message_id": message_id, "topic_id": topic_id},
        )
        assert resolved_key == worker_key
        assert resolved_worker is worker

def test_stable_reply_binding_ignores_positional_worker_id_collision_on_worker_and_space_topics():
    store = _store()
    worker_key, worker, _created = _stable_reply_worker(store)
    _other_key, other, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-other",
                "name": "codex",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-other",
                "meta": {
                    "agent": "codex",
                    "stable_key": "wsk1_" + "b" * 64,
                    "stable_key_version": 1,
                },
            }
        ),
        topic_id="28",
    )
    other["tendwire_worker_id"] = worker["tendwire_worker_id"]
    other["worker_id"] = worker["worker_id"]
    _space_key, space, _created = state.upsert_space_entry(
        store,
        {
            "id": "space-1",
            "name": "Project",
            "status": "active",
            "fingerprint": "space-fp",
        },
        topic_id="77",
    )
    assert state.cache_space_active_worker(space, worker) is True

    stable_key = worker["tendwire_stable_key"]
    assert state.find_worker_entry_by_stable_key(store, stable_key) == (
        worker_key,
        worker,
    )
    assert state.worker_entry_is_uniquely_routable(store, worker_key, worker)
    for message_id, topic_id in (("501", "26"), ("502", "77")):
        state.bind_message_to_worker(
            store,
            message_id,
            worker,
            topic_id=topic_id,
            kind="final",
        )
        resolved_key, resolved_worker = herdres._worker_entry_from_reply(
            store,
            {"reply_to_message_id": message_id, "topic_id": topic_id},
        )
        assert resolved_key == worker_key
        assert resolved_worker is worker



def test_legacy_no_key_reply_binding_requires_resolved_worker_topic_ownership():
    store = _store()
    worker_key, worker, _created = _stable_reply_worker(store)

    for message_id, topic_id in (("500", "99"), ("501", "26")):
        state.bind_message_to_worker(store, message_id, worker, topic_id=topic_id, kind="final")
        binding = store["telegram_message_bindings"][message_id]
        binding.pop("stable_key")
        binding.pop("stable_key_version")

    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "99"},
    ) == (None, None)
    resolved_key, resolved_worker = herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "501", "topic_id": "26"},
    )
    assert resolved_key == worker_key
    assert resolved_worker is worker


def test_command_reply_to_agent_message_targets_original_worker(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    _worker_key, claude, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-claude", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-claude"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-codex")[1]
    )
    state.bind_message_to_worker(store, "555", claude, topic_id="77", kind="final", turn_id="turn-claude")
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "request_id": REQUEST_ID,
            "reply_to_message_id": "555",
            "text": "/send reply to claude",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"}
    assert request["instruction"] == {"text": "reply to claude"}


def test_command_reply_at_alias_targets_worker_in_space(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_worker_entry(store, _source_worker({"id": "worker-claude", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-claude"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-codex")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "request_id": REQUEST_ID,
            "text": "/send @claude hello there",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"}
    assert request["instruction"] == {"text": "hello there"}


def test_command_reply_target_bot_kind_targets_worker_in_space(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_worker_entry(store, _source_worker({"id": "worker-claude", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-claude"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-codex")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "request_id": REQUEST_ID,
            "target_bot_kind": "claude",
            "text": "hello from child bot",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"}
    assert request["instruction"] == {"text": "hello from child bot"}


def test_command_reply_voice_transcript_targets_worker_in_space(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_worker_entry(store, _source_worker({"id": "worker-kimi", "name": "kimi", "status": "idle", "space_id": "space-1", "fingerprint": "fp-kimi"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-codex")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "request_id": REQUEST_ID,
            "target_bot_kind": "kimi",
            "text": "",
            "caption": "",
            "attachment": {"kind": "voice", "file_id": "voice-file", "file_size": 42},
            "_speech_pretranscribed": True,
            "_speech_transcript": "check the worker status",
        }
    )

    assert result == _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-kimi", "worker_fingerprint": "fp-kimi"}
    assert request["instruction"] == {"text": "check the worker status"}
    assert "voice-file" not in json.dumps(request, sort_keys=True)


def test_command_reply_voice_without_transcript_has_voice_specific_reply(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_INPUT", raising=False)
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-1")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "text": "",
            "attachment": {"kind": "voice", "file_id": "voice-file", "file_size": 42},
        }
    )

    assert result["handled"] is True
    assert "Voice transcription is off" in result["reply"]
    assert fake.commands == []


def test_voice_command_sets_space_voice_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    entry["voice_mode"] = "per_agent"
    state.save_state(store)

    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "text": "/voice shared",
        }
    )
    reloaded = state.load_state()
    space = next(iter(state.source_space_entries(reloaded).values()))
    worker = next(iter(state.source_worker_entries(reloaded).values()))

    assert result["voice_mode"] == "shared"
    assert "Voice mode: shared." == result["reply"]
    assert space["voice_mode"] == "shared"
    assert worker["managed_voice_active"] is False


def test_command_reply_unknown_at_alias_fails_safely(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    assert state.cache_space_active_worker(
        entry, state.find_worker_entry_by_id(store, "worker-codex")[1]
    )
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "text": "/send @missing hello",
        }
    )

    assert result["handled"] is True
    assert result["status"] == "unknown_target_alias"
    assert fake.commands == []


def test_gateway_maps_only_source_topic(monkeypatch):
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}), topic_id="78",)
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "text": "hello",
        },
        store,
    )
    assert payload is not None
    assert payload["topic_id"] == "77"
    assert herdres_gateway._payload_for_message({"chat": {"id": "-100"}, "message_thread_id": 78}, store) is None


def test_gateway_child_bot_payload_targets_child_kind(monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token", "username": "codex_bot"}}
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    key = managed_bot_tokens(store["telegram"])[0][0]

    payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "text": "@codex_bot hello",
        },
        store,
        bot_key=key,
    )

    assert payload is not None
    assert payload["target_bot_kind"] == "codex"


def test_gateway_child_bot_does_not_claim_unaddressed_topic_message(monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    key = managed_bot_tokens(store["telegram"])[0][0]

    child_payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "text": "hello",
        },
        store,
        bot_key=key,
    )
    manager_payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "text": "hello",
        },
        store,
    )

    assert child_payload is None
    assert manager_payload is not None
    assert "target_bot_kind" not in manager_payload


def test_gateway_voice_payload_includes_attachment(monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"kimi": {"enabled": True, "token": "kimi-token", "username": "kimi_bot"}}
    state.upsert_worker_entry(store, _source_worker({"id": "worker-kimi", "name": "kimi", "status": "idle", "space_id": "space-1", "fingerprint": "fp-kimi"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    key = managed_bot_tokens(store["telegram"])[0][0]

    payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "voice": {
                "file_id": "voice-file",
                "file_unique_id": "voice-unique",
                "mime_type": "audio/ogg",
                "file_size": 4200,
                "duration": 3,
            },
            "caption": "@kimi_bot",
        },
        store,
        bot_key=key,
    )

    assert payload is not None
    assert payload["target_bot_kind"] == "kimi"
    assert payload["attachment"]["kind"] == "voice"
    assert payload["attachment"]["file_id"] == "voice-file"


def test_gateway_pretranscribes_voice_before_command(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(store, _source_worker({"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    state.save_state(store)
    seen = {}

    def fake_pretranscribe(payload, *, bot_token):
        assert bot_token == "receiver-token"
        out = dict(payload)
        out["_speech_pretranscribed"] = True
        out["_speech_transcript"] = "from voice"
        return out

    def fake_command(payload):
        seen.update(payload)
        return _gateway_child(
            payload["request_id"],
            disposition="terminal_accepted",
        )

    monkeypatch.setattr(herdres_gateway.speech, "pretranscribe_voice_payload", fake_pretranscribe)
    monkeypatch.setattr(herdres_gateway, "run_herdres_command", fake_command)

    checkpoint = herdres_gateway.handle_message(
        {
            "chat": {"id": -100, "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "voice": {"file_id": "voice-file", "file_size": 42},
        },
        "receiver-token",
        update_id=44,
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )

    assert seen["_speech_pretranscribed"] is True
    assert seen["_speech_transcript"] == "from voice"
    assert checkpoint == herdres_gateway.CHECKPOINT_HOLD


def test_gateway_same_update_retries_byte_identical_and_distinct_update_differs(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "0")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
    )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    state.save_state(store)
    requests = []

    class RetryThenAccept(FakeTendwire):
        def command_json(self, request_json):
            request = json.loads(request_json)
            self.commands.append(request)
            if len(self.commands) == 1:
                return {
                    "ok": False,
                    "status": "backend_unavailable",
                    "disposition": "no_receipt",
                }
            return _accepted_command_response(request)

    fake = RetryThenAccept()

    class ClientFactory:
        def __call__(self):
            return fake

    def fake_command(payload):
        result = herdres.command_reply(payload)
        requests.append(
            json.dumps(fake.commands[-1], sort_keys=True, separators=(",", ":"))
        )
        return result

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    monkeypatch.setattr(herdres_gateway, "run_herdres_command", fake_command)
    message = {
        "chat": {"id": -100, "is_forum": True},
        "message_thread_id": 77,
        "message_id": 10,
        "from": {"id": 1, "is_bot": False},
        "text": "identical text",
    }
    first = {"update_id": 44, "message": message}
    distinct = {"update_id": 45, "message": dict(message)}

    checkpoints = [
        herdres_gateway.handle_update(
            update,
            "receiver-token",
            receiver_id="manager",
            request_id_key=REQUEST_ID_KEY,
        )
        for update in (first, first, first, distinct)
    ]

    assert checkpoints == [
        herdres_gateway.CHECKPOINT_RETRY,
        herdres_gateway.CHECKPOINT_HOLD,
        herdres_gateway.CHECKPOINT_HOLD,
        herdres_gateway.CHECKPOINT_HOLD,
    ]

    assert len(requests) == 3
    assert requests[0] == requests[1]
    first_payload = json.loads(requests[0])
    retry_payload = json.loads(requests[1])
    distinct_payload = json.loads(requests[2])
    assert first_payload["request_id"] == retry_payload["request_id"]
    assert first_payload["request_id"] != distinct_payload["request_id"]


def test_gateway_managed_receiver_identity_is_stable_across_token_rotation():
    old_key = "managed-codex-0123456789ab"
    rotated_key = "managed-codex-fedcba987654"
    old_receiver = herdres_gateway._receiver_id_for_key(old_key)
    rotated_receiver = herdres_gateway._receiver_id_for_key(rotated_key)

    assert old_receiver == rotated_receiver == "codex"
    assert derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id=old_receiver,
        update_id=44,
        chat_id=-100,
        message_id=10,
    ) == derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id=rotated_receiver,
        update_id=44,
        chat_id=-100,
        message_id=10,
    )


def test_gateway_replays_cached_request_after_route_disappears(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "0")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-original",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)
    tendwire_calls = []
    child_payloads = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            tendwire_calls.append(request)
            if len(tendwire_calls) == 1:
                return {
                    "ok": False,
                    "status": "backend_unavailable",
                    "disposition": "no_receipt",
                }
            return _accepted_command_response(request)

    def run_child(payload):
        child_payloads.append(json.loads(json.dumps(payload)))
        return herdres.command_reply(payload)

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(herdres_gateway, "run_herdres_command", run_child)
    update = {
        "update_id": 44,
        "message": {
            "chat": {"id": -100, "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": 1, "is_bot": False},
            "text": "original command",
        },
    }

    first = herdres_gateway.handle_update(
        update,
        "receiver-token",
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )
    changed = state.load_state()
    changed["panes"] = {}
    state.save_state(changed)
    replay = herdres_gateway.handle_update(
        update,
        "receiver-token",
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )

    assert first == herdres_gateway.CHECKPOINT_RETRY
    assert replay == herdres_gateway.CHECKPOINT_HOLD
    assert len(tendwire_calls) == 2
    assert tendwire_calls[0] == tendwire_calls[1]
    assert child_payloads[1] == {
        "request_id": child_payloads[0]["request_id"],
    }


def test_gateway_unknown_message_holds_without_tendwire_mutation(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    store = _store()
    state.save_state(store)
    before = state.load_state()
    child_payloads = []
    telegram = FakeTelegram()

    def run_child(payload):
        child_payloads.append(dict(payload))
        return herdres.command_reply(payload)

    monkeypatch.setattr(herdres_gateway, "run_herdres_command", run_child)
    monkeypatch.setattr(
        herdres_gateway, "TelegramClient", lambda **_kwargs: telegram
    )
    checkpoint = herdres_gateway.handle_update(
        {
            "update_id": 45,
            "message": {
                "chat": {"id": -100, "is_forum": True},
                "message_thread_id": 999,
                "message_id": 11,
                "from": {"id": 1, "is_bot": False},
                "text": "not routed",
            },
        },
        "receiver-token",
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )

    assert checkpoint == herdres_gateway.CHECKPOINT_HOLD
    assert set(child_payloads[0]) == {"request_id"}
    after = state.load_state()
    records = after.pop(ingress_requests.RECORDS_KEY)
    assert after == before
    record = records[child_payloads[0]["request_id"]]
    assert record["state"] == "quarantined"
    assert record["request_json"] is None
    assert record["outcome"] == ingress_requests.child_result(
        child_payloads[0]["request_id"],
        checkpoint="hold",
        transport_disposition=None,
        request_phase="terminal",
        terminal_outcome="not_delivered",
        reply=herdres.REKEYED_TOPIC_QUARANTINE_REPLY,
    )
    assert telegram.sent == []


@pytest.mark.parametrize(
    ("result", "checkpoint"),
    [
        (_gateway_child(REQUEST_ID), herdres_gateway.CHECKPOINT_ADVANCE),
        (
            _gateway_child(REQUEST_ID, handled=False),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            _gateway_child(REQUEST_ID, disposition="terminal_accepted"),
            herdres_gateway.CHECKPOINT_HOLD,
        ),
        (
            _gateway_child(REQUEST_ID, disposition="terminal_rejected"),
            herdres_gateway.CHECKPOINT_HOLD,
        ),
        (
            _gateway_child(REQUEST_ID, disposition="terminal_uncertain"),
            herdres_gateway.CHECKPOINT_HOLD,
        ),
        (
            _gateway_child(
                REQUEST_ID,
                checkpoint=herdres_gateway.CHECKPOINT_RETRY,
                disposition=None,
            ),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            _gateway_child(
                REQUEST_ID,
                checkpoint=herdres_gateway.CHECKPOINT_RETRY,
                disposition="no_receipt",
            ),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            _gateway_child(
                REQUEST_ID,
                checkpoint=herdres_gateway.CHECKPOINT_RETRY,
                disposition="in_progress",
            ),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            _gateway_child(
                REQUEST_ID,
                checkpoint=herdres_gateway.CHECKPOINT_RETRY,
                disposition="terminal_uncertain",
            ),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            _gateway_child(
                REQUEST_ID,
                checkpoint=herdres_gateway.CHECKPOINT_ADVANCE,
                disposition="no_receipt",
            ),
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            {
                **_gateway_child(
                    REQUEST_ID,
                    checkpoint=herdres_gateway.CHECKPOINT_RETRY,
                ),
                "reply": "must not acknowledge",
            },
            herdres_gateway.CHECKPOINT_RETRY,
        ),
        (
            {"schema_version": 1, "request_id": REQUEST_ID},
            herdres_gateway.CHECKPOINT_RETRY,
        ),
    ],
)
def test_gateway_command_checkpoint_disposition_matrix(result, checkpoint):
    assert (
        herdres_gateway._checkpoint_for_command_result(
            result,
            request_id=REQUEST_ID,
        )
        == checkpoint
    )


@pytest.mark.parametrize(
    ("returncode", "mutate"),
    [
        (1, lambda body: None),
        (2, lambda body: None),
        (0, lambda body: body.update({"request_id": REQUEST_ID_2})),
        (0, lambda body: body.update({"extra": "not allowed"})),
        (
            0,
            lambda body: body.update(
                {
                    "checkpoint": herdres_gateway.CHECKPOINT_RETRY,
                    "disposition": "terminal_uncertain",
                }
            ),
        ),
    ],
)
def test_gateway_child_exit_shape_and_correlation_fail_closed(
    monkeypatch,
    returncode,
    mutate,
):
    body = _gateway_child(
        REQUEST_ID,
        disposition="terminal_accepted",
        reply="accepted",
    )
    mutate(body)
    completed = type(
        "Completed",
        (),
        {
            "returncode": returncode,
            "stdout": json.dumps(body).encode(),
            "stderr": b"private child details",
        },
    )()
    monkeypatch.setattr(
        herdres_gateway.subprocess,
        "run",
        lambda *_args, **_kwargs: completed,
    )

    result = herdres_gateway.run_herdres_command(
        {"request_id": REQUEST_ID}
    )
    assert isinstance(result, ingress_requests.IngressResult)
    assert result == _gateway_child(
        REQUEST_ID,
        checkpoint=herdres_gateway.CHECKPOINT_RETRY,
    )


def test_gateway_accepts_exact_correlated_child_envelope(monkeypatch):
    expected = _gateway_child(
        REQUEST_ID,
        disposition="terminal_rejected",
        reply="Could not send safely.",
    )
    completed = type(
        "Completed",
        (),
        {
            "returncode": 0,
            "stdout": json.dumps(expected).encode(),
            "stderr": b"",
        },
    )()
    monkeypatch.setattr(
        herdres_gateway.subprocess,
        "run",
        lambda *_args, **_kwargs: completed,
    )

    result = herdres_gateway.run_herdres_command(
        {"request_id": REQUEST_ID}
    )
    assert isinstance(result, ingress_requests.IngressResult)
    assert result == expected


def test_gateway_invalid_utf8_child_output_is_uncertain(monkeypatch):
    completed = type(
        "Completed",
        (),
        {"returncode": 0, "stdout": b"\xff", "stderr": b""},
    )()
    monkeypatch.setattr(
        herdres_gateway.subprocess,
        "run",
        lambda *_args, **_kwargs: completed,
    )

    result = herdres_gateway.run_herdres_command({"request_id": REQUEST_ID})

    assert isinstance(result, ingress_requests.IngressResult)
    assert result == _gateway_child(
        REQUEST_ID,
        checkpoint=herdres_gateway.CHECKPOINT_RETRY,
    )
    assert (
        herdres_gateway._checkpoint_for_command_result(
            result,
            request_id=REQUEST_ID,
        )
        == herdres_gateway.CHECKPOINT_RETRY
    )


def test_gateway_fsyncs_first_seen_shell_before_route_or_child(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    monkeypatch.setenv("HERDRES_COMMAND_RETRY_HORIZON_SECONDS", "60")
    state.save_state(_store())
    monkeypatch.setattr(herdres_gateway.time, "time", lambda: 100.0)
    request_id = derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id="manager",
        update_id=44,
        chat_id=-100,
        message_id=10,
    )
    observations = []

    def inspect_route(_message, _store, *, bot_key=None):
        record = state.load_state()[ingress_requests.RECORDS_KEY][request_id]
        observations.append(("route", record.copy()))
        return None

    def inspect_child(payload):
        record = state.load_state()[ingress_requests.RECORDS_KEY][request_id]
        observations.append(("child", record.copy()))
        assert payload == {"request_id": request_id}
        return _gateway_child(
            request_id,
            checkpoint=herdres_gateway.CHECKPOINT_RETRY,
        )

    monkeypatch.setattr(
        herdres_gateway,
        "_payload_for_message",
        inspect_route,
    )
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        inspect_child,
    )

    checkpoint = herdres_gateway.handle_update(
        {
            "update_id": 44,
            "message": {
                "chat": {"id": -100},
                "message_id": 10,
                "from": {"id": 1, "is_bot": False},
                "text": "one command",
            },
        },
        "receiver-token",
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )

    assert checkpoint == herdres_gateway.CHECKPOINT_RETRY
    assert [kind for kind, _record in observations] == ["route", "child"]
    for _kind, record in observations:
        assert record["schema_version"] == 4
        assert record["request_id"] == request_id
        assert record["created_at"] == 100.0
        assert record["updated_at"] == 100.0
        assert record["deadline_at"] == 160.0
        assert record["retain_until"] == 86_560.0
        assert record["state"] == "resolving"
        assert record["request_json"] is None


def test_gateway_restart_uses_equality_quarantine_and_holds_without_child(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    monkeypatch.setenv("HERDRES_COMMAND_RETRY_HORIZON_SECONDS", "60")
    state.save_state(_store())
    request_id = derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id="manager",
        update_id=44,
        chat_id=-100,
        message_id=10,
    )
    _record, first_outcome = herdres_gateway._preflight_ingress_request(
        request_id,
        now=100.0,
    )
    record, equality_outcome = herdres_gateway._preflight_ingress_request(
        request_id,
        now=160.0,
    )
    assert first_outcome is None
    assert equality_outcome == ingress_requests.child_result(
        request_id,
        checkpoint="hold",
        transport_disposition=None,
        request_phase="terminal",
        terminal_outcome="not_delivered",
        reply=ingress_requests.QUARANTINE_REPLY,
    )
    assert record["state"] == "quarantined"
    assert record["created_at"] == 100.0
    assert record["deadline_at"] == 160.0
    assert record["retain_until"] == 86_560.0
    assert record["updated_at"] == record["quarantined_at"] == 160.0

    update = {
        "update_id": 44,
        "message": {
            "chat": {"id": -100},
            "message_id": 10,
            "from": {"id": 1, "is_bot": False},
            "text": "one command",
        },
    }
    child_calls = []
    saved_offsets = []

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres_gateway.time, "time", lambda: 161.0)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: child_calls.append(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 44)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, _offset, *, timeout_seconds: [update],
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda offset, key: saved_offsets.append((offset, key)),
    )

    herdres_gateway._poll_once(
        "manager",
        "receiver-token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )

    assert child_calls == []
    assert saved_offsets == []
    restarted = state.load_state()[ingress_requests.RECORDS_KEY][request_id]
    assert restarted["state"] == "quarantined"
    assert restarted["updated_at"] == 160.0
    assert restarted["outcome"] == equality_outcome


def test_gateway_corrupt_existing_record_is_preserved_global_barrier(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    request_id = derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id="manager",
        update_id=44,
        chat_id=-100,
        message_id=10,
    )
    corrupt_record = {
        "created_at": -100_000.0,
        "request_json": '{"private":"untrusted"}',
    }
    store = _store()
    store[ingress_requests.RECORDS_KEY] = {request_id: corrupt_record}
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()
    child_calls = []
    reply_attempts = []

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            reply_attempts.append(self.token)
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres_gateway.time, "time", lambda: 101.0)
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: child_calls.append(payload),
    )

    update = {
        "update_id": 44,
        "message": {
            "chat": {"id": -100},
            "message_id": 10,
            "from": {"id": 1, "is_bot": False},
            "text": "must not reconstruct",
        },
    }
    for _attempt in range(2):
        with pytest.raises(
            RuntimeError, match="^ingress request record store is corrupt$"
        ):
            herdres_gateway.handle_update(
                update,
                "receiver-token",
                receiver_id="manager",
                request_id_key=REQUEST_ID_KEY,
            )

    assert child_calls == []
    assert reply_attempts == []
    assert state_path.read_bytes() == original
    assert (
        state.load_state()[ingress_requests.RECORDS_KEY][request_id]
        == corrupt_record
    )


def test_gateway_terminal_uncertain_update_holds_checkpoint_and_next_update(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "0")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)
    command_requests = []
    saved_offsets = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            command_requests.append(request)
            if len(command_requests) == 1:
                return {
                    "ok": False,
                    "status": "request_state_uncertain",
                    "disposition": "terminal_uncertain",
                }
            return _accepted_command_response(request)

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            return {"ok": True, "message_id": "1"}

    def run_child(payload):
        return herdres.command_reply(payload)

    updates = [
        {
            "update_id": update_id,
            "message": {
                "chat": {"id": -100, "is_forum": True},
                "message_thread_id": 77,
                "message_id": message_id,
                "from": {"id": 1, "is_bot": False},
                "text": text,
            },
        }
        for update_id, message_id, text in (
            (44, 10, "first command"),
            (45, 11, "second command"),
        )
    ]
    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(herdres_gateway, "run_herdres_command", run_child)
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 44)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, _offset, *, timeout_seconds: updates,
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda offset, key: saved_offsets.append((offset, key)),
    )

    herdres_gateway._poll_once(
        "manager",
        "receiver-token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )

    assert len(command_requests) == 1
    assert saved_offsets == []
    records = state.load_state()[ingress_requests.RECORDS_KEY]
    first = records[command_requests[0]["request_id"]]
    assert first["state"] == "quarantined"
    assert first["transport_disposition"] == "terminal_uncertain"
    assert first["terminal_outcome"] == "delivery_unknown"
    assert first["outcome"]["checkpoint"] == herdres_gateway.CHECKPOINT_HOLD
    assert first["operator_attention_required"] is True


@pytest.mark.parametrize("delivery_failure", ["explicit", "raised"])
@pytest.mark.parametrize(
    ("command_outcome", "record_state"),
    [
        ("terminal", "terminal"),
        ("quarantine", "quarantined"),
    ],
)
def test_gateway_unverified_outcome_holds_and_redelivery_is_cached(
    monkeypatch,
    tmp_path,
    delivery_failure,
    command_outcome,
    record_state,
):
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE",
        str(tmp_path / "state.json"),
    )
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "1")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)
    backend_requests = []
    reply_attempts = []
    saved_offsets = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            backend_requests.append(request)
            if command_outcome == "terminal":
                return _accepted_command_response(request)
            return _failed_command_response(
                request,
                status="request_state_uncertain",
                disposition="terminal_uncertain",
            )

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, chat_id, reply, **kwargs):
            reply_attempts.append((chat_id, reply, kwargs))
            if delivery_failure == "raised":
                raise RuntimeError("private permanent Telegram delivery failure")
            return {"ok": False, "error": "private permanent Telegram delivery failure"}

    updates = [
        {
            "update_id": 44,
            "message": {
                "chat": {"id": -100, "is_forum": True},
                "message_thread_id": 77,
                "message_id": 10,
                "from": {"id": 1, "is_bot": False},
                "text": "one command",
            },
        },
        {
            "update_id": 45,
            "edited_message": {
                "chat": {"id": -100},
                "message_id": 11,
                "text": "next update",
            },
        },
    ]

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 44)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, _offset, *, timeout_seconds: updates,
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda offset, key: saved_offsets.append((offset, key)),
    )

    for _delivery in range(2):
        herdres_gateway._poll_once(
            "manager",
            "receiver-token",
            timeout_seconds=0,
            request_id_key=REQUEST_ID_KEY,
        )

    assert saved_offsets == []
    assert len(backend_requests) == 1
    request_id = backend_requests[0]["request_id"]
    cached = state.load_state()[ingress_requests.RECORDS_KEY][request_id]
    assert cached["state"] == record_state
    assert cached["outcome"]["checkpoint"] == herdres_gateway.CHECKPOINT_HOLD
    assert cached["operator_attention_required"] is True
    assert reply_attempts == []


def test_gateway_uncertain_result_is_not_acknowledged(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)
    replies = []

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            replies.append(self.token)

    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: _gateway_child(
            payload["request_id"],
            checkpoint=herdres_gateway.CHECKPOINT_RETRY,
        ),
    )

    checkpoint = herdres_gateway.handle_update(
        {
            "update_id": 44,
            "message": {
                "chat": {"id": -100},
                "message_thread_id": 77,
                "message_id": 10,
                "from": {"id": 1, "is_bot": False},
                "text": "one command",
            },
        },
        "receiver-token",
        receiver_id="manager",
        request_id_key=REQUEST_ID_KEY,
    )

    assert checkpoint == herdres_gateway.CHECKPOINT_RETRY
    assert replies == []


def test_gateway_poll_checkpoints_only_terminal_updates(monkeypatch):
    updates = [
        {"update_id": 44, "message": {}},
        {"update_id": 45, "message": {}},
    ]
    saved = []
    handled = []
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 44)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, _offset, *, timeout_seconds: updates,
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda offset, key: saved.append((offset, key)),
    )

    def uncertain(update, *_args, **_kwargs):
        handled.append(update["update_id"])
        return herdres_gateway.CHECKPOINT_RETRY

    monkeypatch.setattr(herdres_gateway, "handle_update", uncertain)
    herdres_gateway._poll_once(
        "manager",
        "token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )
    assert handled == [44]
    assert saved == []

    handled.clear()
    monkeypatch.setattr(
        herdres_gateway,
        "handle_update",
        lambda update, *_args, **_kwargs: (
            handled.append(update["update_id"])
            or herdres_gateway.CHECKPOINT_ADVANCE
        ),
    )
    herdres_gateway._poll_once(
        "manager",
        "token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )
    assert handled == [44, 45]
    assert saved == [(45, "manager"), (46, "manager")]


def test_gateway_manager_skips_reply_owned_by_child_bot(monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    _worker_key, worker, _created = state.upsert_worker_entry(store, _source_worker({"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"}), )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    state.bind_message_to_worker(store, "555", worker, topic_id="77", kind="final", turn_id="turn-1", bot_kind="codex")

    payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "reply_to_message": {"message_id": 555},
            "from": {"id": "1", "is_bot": False},
            "text": "reply to codex",
        },
        store,
    )

    assert payload is None


def test_gateway_offset_is_stable_across_rotation_and_migrates_oldest_legacy(
    tmp_path,
    monkeypatch,
):
    base = tmp_path / "gateway.offset"
    monkeypatch.setattr(herdres_gateway.config, "offset_path", lambda: base)
    oldest_key = "managed-codex-0123456789ab"
    newer_key = "managed-codex-111111111111"
    rotated_key = "managed-codex-fedcba987654"
    oldest_legacy = herdres_gateway._legacy_offset_path_for(oldest_key)
    newer_legacy = herdres_gateway._legacy_offset_path_for(newer_key)
    oldest_legacy.write_text("44", encoding="utf-8")
    newer_legacy.write_text("51", encoding="utf-8")
    seen_offsets = []

    def get_updates(_token, offset, *, timeout_seconds):
        assert timeout_seconds == 0
        seen_offsets.append(offset)
        return []

    monkeypatch.setattr(herdres_gateway, "get_updates", get_updates)
    monkeypatch.setattr(
        herdres_gateway,
        "_drain_backlog",
        lambda *_args: pytest.fail("retained legacy offset must not drain backlog"),
    )

    stable_path = herdres_gateway._offset_path_for(rotated_key)
    herdres_gateway._poll_once(
        rotated_key,
        "rotated-token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )

    assert seen_offsets == [44]
    assert stable_path.name == "gateway.offset.codex"
    assert stable_path.read_text(encoding="utf-8") == "44"
    assert not oldest_legacy.exists()
    assert not newer_legacy.exists()

    herdres_gateway._save_offset(45, rotated_key)
    assert herdres_gateway._read_offset(rotated_key) == 45


@pytest.mark.parametrize(
    ("legacy_suffix", "contents"),
    [
        ("managed-codex-0123456789ab", "not-an-offset"),
        ("managed-codex-not-a-token-digest", "44"),
    ],
)
def test_gateway_invalid_legacy_offset_evidence_never_drains_backlog(
    tmp_path,
    monkeypatch,
    legacy_suffix,
    contents,
):
    base = tmp_path / "gateway.offset"
    monkeypatch.setattr(herdres_gateway.config, "offset_path", lambda: base)
    evidence = base.with_name(f"{base.name}.{legacy_suffix}")
    evidence.write_text(contents, encoding="utf-8")
    drains = []
    monkeypatch.setattr(
        herdres_gateway,
        "_drain_backlog",
        lambda *args: drains.append(args),
    )

    with pytest.raises(RuntimeError, match="offset"):
        herdres_gateway._poll_once(
            "managed-codex-fedcba987654",
            "rotated-token",
            timeout_seconds=0,
            request_id_key=REQUEST_ID_KEY,
        )

    assert drains == []
    assert not herdres_gateway._offset_path_for(
        "managed-codex-fedcba987654"
    ).exists()


def test_gateway_first_start_without_offset_evidence_still_drains_backlog(
    tmp_path,
    monkeypatch,
):
    base = tmp_path / "gateway.offset"
    monkeypatch.setattr(herdres_gateway.config, "offset_path", lambda: base)
    drains = []

    def drain(key, token):
        drains.append((key, token))
        return 90

    monkeypatch.setattr(herdres_gateway, "_drain_backlog", drain)

    herdres_gateway._poll_once(
        "managed-codex-fedcba987654",
        "current-token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
    )

    assert drains == [
        ("managed-codex-fedcba987654", "current-token"),
    ]


def test_runtime_has_no_direct_herdr_pane_api_names():
    forbidden = [
        "pane_list",
        "pane_by_id",
        "pane_turn",
        "prefetch_pane_turns",
        "send_to_pane",
        "pane send-keys",
        "pane read",
    ]
    runtime_files = [Path("herdres.py"), Path("herdres_gateway.py"), *Path("herdres_connector").glob("*.py")]
    text = "\n".join(path.read_text(encoding="utf-8") for path in runtime_files)
    for needle in forbidden:
        assert needle not in text


def test_public_prune_removes_private_fields():
    payload = {
        "ok": True,
        "chat_id": "-100",
        "topic_id": "77",
        "message_id": "10",
        "token": "secret",
        "target": {"worker_id": "w", "backend_target": "raw"},
    }
    clean = public_prune(payload)
    encoded = json.dumps(clean)
    assert "-100" not in encoded
    assert "secret" not in encoded
    assert "backend_target" not in encoded


def test_placeholder_turn_never_outranks_real_turn_for_same_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-empty-placeholder",
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "complete": False,
                    "has_open_turn": True,
                },
                {
                    "id": "turn-real",
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "user_text": "please do the thing",
                    "assistant_stream_text": "thinking about the thing",
                    "complete": False,
                    "has_open_turn": True,
                },
            ]
        },
        workers=[
            {"id": "worker-1", "name": "claude", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"}
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    working_cards = [html for _chat, html, _kw, _mid in telegram.sent if "Working" in html or "Work is in progress" in html]
    assert len(working_cards) == 1
    assert "thinking about the thing" in working_cards[0]
    assert "Work is in progress." not in working_cards[0]


def test_later_structured_turn_reuses_placeholder_working_card_and_final(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    worker = {
        "id": "worker-1",
        "name": "codex",
        "status": "working",
        "space_id": "space-1",
        "fingerprint": "fp-1",
    }
    spaces = [
        {
            "id": "space-1",
            "name": "Project",
            "status": "active",
            "fingerprint": "space-fp",
        }
    ]
    placeholder = {
        "id": "turn-placeholder",
        "worker_id": "worker-1",
        "space_id": "space-1",
        "user_text": "how should I promote this?",
        "complete": False,
        "has_open_turn": True,
    }

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [placeholder]}, workers=[worker], spaces=spaces),
            telegram,
            with_outbox=False,
        ),
    )
    working_cards = [
        item for item in telegram.sent if "Work is in progress." in item[1]
    ]
    assert len(working_cards) == 1
    working_message_id = str(working_cards[0][3])

    structured = {
        "id": "turn-structured",
        "worker_id": "worker-1",
        "space_id": "space-1",
        "source_turn_id": "turnsrc-structured",
        "user_text": "how should I promote this?",
        "assistant_stream_text": "checking the marketplace rules",
        "complete": False,
        "has_open_turn": True,
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                turns={"turns": [structured, placeholder]},
                workers=[worker],
                spaces=spaces,
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert len(
        [item for item in telegram.sent if "Working" in item[1]]
    ) == 1
    assert telegram.edited[-1][1] == working_message_id
    assert "checking the marketplace rules" in telegram.edited[-1][2]
    entry = next(
        item
        for item in state.source_worker_entries(store).values()
        if item.get("tendwire_worker_id") == "worker-1"
    )
    assert entry["last_stream_turn_id"] == "turn-structured"
    assert str(entry["last_stream_message_id"]) == working_message_id
    binding = state.find_message_binding(store, working_message_id)
    assert binding is not None
    assert binding["turn_id"] == "turn-structured"

    final = {
        **structured,
        "assistant_stream_text": None,
        "assistant_final_text": "Use one combined Herdres plugin.",
        "complete": True,
        "has_open_turn": False,
    }
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(turns={"turns": [final]}, workers=[worker], spaces=spaces),
            telegram,
            with_outbox=False,
        ),
    )

    assert len(
        [item for item in telegram.sent if "Working" in item[1]]
    ) == 1
    assert telegram.edited[-1][1] == working_message_id
    assert "Use one combined Herdres plugin." in telegram.edited[-1][2]


def test_retired_worker_turns_are_not_delivered(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-retired",
                    "worker_id": "worker-retired",
                    "space_id": "space-1",
                    "user_text": "old prompt",
                    "assistant_final_text": "stale final from a retired worker id",
                    "complete": True,
                }
            ]
        },
        workers=[
            {"id": "worker-live", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-live"}
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    assert all("stale final" not in html for _chat, html, _kw, _mid in telegram.sent)


def test_closed_worker_turns_are_not_delivered(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-closed-worker",
                    "worker_id": "worker-phantom",
                    "space_id": "space-1",
                    "complete": False,
                    "has_open_turn": True,
                }
            ]
        },
        workers=[
            {"id": "worker-phantom", "name": "codex", "status": "closed", "space_id": "space-1", "fingerprint": "fp-ghost"},
            {"id": "worker-live", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-live"},
        ],
        spaces=[{"id": "space-1", "name": "projectx", "status": "active", "fingerprint": "space-fp"}],
    )

    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    assert all("Work is in progress" not in html for _chat, html, _kw, _mid in telegram.sent)


def test_attention_status_flips_alert_icon_and_recovery_restores_identity(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()

    def tendwire_with_status(status):
        return FakeTendwire(
            turns={"turns": []},
            workers=[{"id": "worker-1", "name": "Alpha", "status": status, "space_id": "space-1", "fingerprint": "fp-1"}],
        )

    # First sync: identity icon assigned once.
    sync_once(store, SyncRuntime(tendwire_with_status("working"), telegram, with_outbox=False))
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]

    # Routine flips never touch the icon again.
    sync_once(store, SyncRuntime(tendwire_with_status("idle"), telegram, with_outbox=False))
    sync_once(store, SyncRuntime(tendwire_with_status("working"), telegram, with_outbox=False))
    assert telegram.icon_edits == [("-100", "77", "icon-fox")]

    # Attention flips to the alert icon.
    sync_once(store, SyncRuntime(tendwire_with_status("attention"), telegram, with_outbox=False))
    assert telegram.icon_edits[-1] == ("-100", "77", "icon-attention")

    # Recovery restores the identity icon.
    sync_once(store, SyncRuntime(tendwire_with_status("idle"), telegram, with_outbox=False))
    assert telegram.icon_edits[-1] == ("-100", "77", "icon-fox")
    entry = next(iter(state.source_space_entries(store).values()))
    assert entry["last_topic_icon"] == "🦊"


def test_new_topics_are_created_with_deterministic_color(monkeypatch):
    from herdres_connector.source_sync import topic_color_for_name
    from herdres_connector.telegram_delivery import TOPIC_ICON_COLORS

    color = topic_color_for_name("demoapp")
    assert color in TOPIC_ICON_COLORS
    assert topic_color_for_name("demoapp") == color  # deterministic
