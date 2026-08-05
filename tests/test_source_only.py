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
from herdres_connector import doctor, source_sync, state
from herdres_connector.rendering import render_pending, render_status_overview
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
                        owner
                        in {
                            "_execute_accounted_delivery_write",
                            "_execute_decision_provider_operation",
                        }
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
            "tendwire.connector_fail: reject unsupported exact leased event"
        ),
        ("connector_fail", 1): (
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




def _delivery_write_accounting_violations(source_text):
    """Keep the staged-final provider inventory literal and budgeted."""

    tree = ast.parse(source_text)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    expected = {
        ("_delete_final_message", "telegram.delete_turn_delivery_message"),
        ("_send_or_edit_final_part", "telegram.edit_turn_delivery_part"),
        ("_send_or_edit_final_part", "telegram.send_turn_delivery_part"),
    }
    seen = set()
    violations = []
    expected_owners = {name for name, _capability in expected}
    for owner, function in functions.items():
        for node in ast.walk(function):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_provider_mutation"
                and node.args
            ):
                continue
            capability_node = node.args[0]
            if not (
                isinstance(capability_node, ast.Constant)
                and isinstance(capability_node.value, str)
            ):
                if owner in expected_owners:
                    violations.append(
                        ("dynamic_delivery_capability", node.lineno, owner)
                    )
                continue
            key = (owner, capability_node.value)
            if key in expected:
                seen.add(key)
    for missing in sorted(expected - seen):
        violations.append(("missing_staged_delivery_write", *missing))
    final_sender = functions.get("_send_or_edit_final_part")
    if final_sender is not None and (
        "max_operations - result['operations']"
        not in ast.unparse(final_sender)
    ):
        violations.append(
            (
                "staged_delivery_write_without_exact_allowance",
                final_sender.lineno,
                final_sender.name,
            )
        )
    return violations


def test_source_sync_mutations_require_offlock_executor():
    """The off-lock protocol is enforced structurally, not by convention."""

    source_path = Path(source_sync.__file__)
    delivery_path = source_path.with_name("telegram_delivery.py")
    source_text = source_path.read_text(encoding="utf-8")
    delivery_text = delivery_path.read_text(encoding="utf-8")
    assert _offlock_protocol_violations(source_text, delivery_text) == []




def test_delivery_write_accounting_is_structural_and_fails_closed():
    source_path = Path(source_sync.__file__)
    source_text = source_path.read_text(encoding="utf-8")
    assert _delivery_write_accounting_violations(source_text) == []

    assembled = source_text.replace(
        '"telegram.send_turn_delivery_part",\n                reason=(',
        '"telegram." + "send_turn_delivery_part",\n                reason=(',
        1,
    )
    assert any(
        violation[0] == "dynamic_delivery_capability"
        for violation in _delivery_write_accounting_violations(assembled)
    )

    fixed_allowance = source_text.replace(
        'max(\n            1, max_operations - result["operations"]\n        )',
        "1",
        1,
    )
    assert any(
        violation[0]
        == "staged_delivery_write_without_exact_allowance"
        for violation in _delivery_write_accounting_violations(
            fixed_allowance
        )
    )


def _pending_delivery_work_components(source_text):
    tree = ast.parse(source_text)
    sync_function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "sync_once"
    )
    assignment = next(
        node
        for node in ast.walk(sync_function)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id == "pending_delivery_work"
            for target in node.targets
        )
    )
    components = set()
    for call in ast.walk(assignment.value):
        if not (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "get"
            and isinstance(call.func.value, ast.Name)
            and call.args
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        ):
            continue
        components.add((call.func.value.id, call.args[0].value))
    return components


def test_pending_delivery_work_inventory_is_complete_and_fail_closed():
    source_text = Path(source_sync.__file__).read_text(encoding="utf-8")
    expected = {
        ("turn_counts", "work_pending"),
        ("turn_final_result", "failed"),
        ("turn_final_result", "deferred"),
        ("outbox_result", "failed"),
        ("outbox_result", "deferred"),
    }
    assert _pending_delivery_work_components(source_text) == expected

    missing_outbox_failure = source_text.replace(
        '        + int(outbox_result.get("failed") or 0)\n',
        "",
        1,
    )
    assert (
        _pending_delivery_work_components(missing_outbox_failure)
        == expected - {("outbox_result", "failed")}
    )


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


def _stable_target(worker_id: str, fingerprint: str) -> dict[str, object]:
    material = f"{worker_id}\0{fingerprint}"
    return {
        "stable_key": "wsk1_" + hashlib.sha256(material.encode()).hexdigest(),
        "stable_key_version": 1,
    }


def _worker_target(worker_id: str, fingerprint: str) -> dict[str, str]:
    return {
        "worker_id": worker_id,
        "worker_fingerprint": fingerprint,
    }


def _accepted_command_response(request):
    target = request.get("target", {})
    worker_id = str(target.get("worker_id") or "worker-1")
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
        self._turns.setdefault("schema_version", 2)
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
        self.delta_calls = []

    def snapshot(self):
        return {
            "ok": True,
            "spaces": self._spaces,
            "workers": self._workers,
        }

    def turns(self):
        return self._turns

    @staticmethod
    def _field_descriptor(value):
        text = value if isinstance(value, str) else ""
        return {
            "availability": "complete" if value is not None else "absent",
            "inline": value is not None,
            "char_length": len(text),
            "byte_length": len(text.encode("utf-8")),
            "page_count": 1 if value is not None else 0,
            "first_cursor": None,
        }

    def _delta_row(self, raw):
        if not isinstance(raw, dict):
            return raw
        row = copy.deepcopy(raw)
        worker = next(
            (
                candidate
                for candidate in self._workers
                if str(candidate.get("id") or "")
                == str(row.get("worker_id") or "")
            ),
            None,
        )
        if isinstance(worker, dict):
            row.setdefault("worker_fingerprint", worker.get("fingerprint"))
            row.setdefault("space_id", worker.get("space_id"))
            meta = worker.get("meta")
            if isinstance(meta, dict):
                if meta.get("stable_key") is not None:
                    row.setdefault("stable_key", meta["stable_key"])
                if meta.get("stable_key_version") is not None:
                    row.setdefault(
                        "stable_key_version", meta["stable_key_version"]
                    )
        if "content" not in row:
            revision = hashlib.sha256(
                json.dumps(row, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            row["content"] = {
                "schema_version": 1,
                "content_revision": f"twrev1.fake_{revision}",
                "known_incomplete": False,
                "fields": {
                    "user_text": self._field_descriptor(
                        row.get("user_text")
                    ),
                    "assistant_final_text": self._field_descriptor(
                        row.get("assistant_final_text")
                    ),
                },
            }
        return row

    def turn_delta(self, *, cursor=None, watermark=None, limit=500):
        self.delta_calls.append(
            {"cursor": cursor, "watermark": watermark, "limit": limit}
        )
        assert cursor is None
        assert limit > 0
        rows = [self._delta_row(raw) for raw in self._turns.get("turns", [])]
        revision = hashlib.sha256(
            json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        checkpoint = f"twdelta1.fake_{revision}"
        changes = []
        if watermark != checkpoint:
            for row in rows:
                if not isinstance(row, dict):
                    changes.append(row)
                    continue
                turn_id = str(row.get("id") or row.get("turn_id") or "")
                changes.append(
                    {
                        "op": "upsert",
                        "turn_id": turn_id,
                        "changed_at": "2030-01-01T00:00:00Z",
                        "turn": row,
                    }
                )
        return {
            "schema_version": 1,
            "projection_schema_version": 2,
            "host_id": "shared-fake-tendwire",
            "mode": "bootstrap" if watermark is None else "changes",
            "changes": changes,
            "has_more": False,
            "next_cursor": None,
            "checkpoint": checkpoint,
            "aggregate": {
                "journal_rows_scanned": len(changes),
                "projection_rows_read": len(rows),
                "changes_returned": len(changes),
                "duration_ms": 1,
            },
        }

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
        "tendwire_delta_sync": {
            "schema_version": 1,
            "projection_schema_version": 2,
            "status": "active",
            "watermark": "twdelta1.test_baseline",
            "pending_cursor": None,
            "projection": {},
            "bootstrap_state": None,
            "failure_count": 0,
            "watermark_updated_at": 4102444800,
            "last_full_reconcile_at": 4102444800,
        },
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


def test_transient_space_absence_retains_topic_identity_and_reuses_it(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    store = _store()
    telegram = FakeTelegram()
    live = FakeTendwire()

    sync_once(store, SyncRuntime(live, telegram, with_outbox=False))
    original_key, original = next(iter(state.source_space_entries(store).items()))
    original_topic = str(original["topic_id"])
    assert len(telegram.topics) == 1

    absent = FakeTendwire(workers=[], spaces=[])
    sync_once(store, SyncRuntime(absent, telegram, with_outbox=False))
    retained = state.source_space_entries(store)[original_key]
    assert retained["stale_space_topic"] is True
    assert str(retained["topic_id"]) == original_topic
    assert telegram.deleted_topics == []

    repeated_absence = sync_once(
        store, SyncRuntime(absent, telegram, with_outbox=False)
    )
    assert repeated_absence["topic_cleanup"]["changed"] is False

    sync_once(store, SyncRuntime(live, telegram, with_outbox=False))
    restored = state.source_space_entries(store)[original_key]
    assert "stale_space_topic" not in restored
    assert str(restored["topic_id"]) == original_topic
    assert len(telegram.topics) == 1


def test_ambiguous_topic_create_is_quarantined_and_never_retried(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")

    class AmbiguousCreateTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.create_attempts = 0

        def create_topic(self, _chat_id, _name, icon_color=None):
            self.create_attempts += 1
            return {
                "ok": False,
                "error": "timed out after submit",
                "ambiguous_acceptance": True,
            }

    store = _store()
    telegram = AmbiguousCreateTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)

    sync_once(store, runtime)
    sync_once(store, runtime)

    entry = next(iter(state.source_space_entries(store).values()))
    assert telegram.create_attempts == 1
    assert entry["binding_state"] == "quarantined:ambiguous_topic_create"
    assert entry["ambiguous_topic_create_name"] == "Project"
    assert "topic_id" not in entry


def test_worker_ambiguous_topic_create_survives_refresh_and_restart(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))

    class AmbiguousCreateTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.create_attempts = 0

        def create_topic(self, _chat_id, _name, icon_color=None):
            self.create_attempts += 1
            return {
                "ok": False,
                "error": "connection closed after submit",
                "ambiguous_acceptance": True,
            }

    telegram = AmbiguousCreateTelegram()
    store = _store()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
    )
    state.save_state(store, state_path)
    restarted = state.load_state(state_path)

    sync_once(
        restarted,
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
    )

    entry = next(iter(state.source_worker_entries(restarted).values()))
    entry["binding_state"] = "quarantined:ambiguous_route"
    sync_once(
        restarted,
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
    )

    assert telegram.create_attempts == 1
    assert entry["binding_state"] == "quarantined:ambiguous_topic_create"
    assert any(
        record.get("kind") == "ambiguous_created_topic"
        for record in restarted["telegram"][
            "accepted_created_topics"
        ].values()
    )


def test_ambiguous_create_owner_churn_keeps_durable_quarantine(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    worker = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "idle",
            "space_id": "space-1",
            "fingerprint": "fp-before",
        }
    )
    _key, entry, _created = state.upsert_worker_entry(store, worker)

    class ChurningAmbiguousTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.create_attempts = 0

        def create_topic(self, _chat_id, _name, icon_color=None):
            self.create_attempts += 1
            entry["tendwire_fingerprint"] = "fp-after"
            return {
                "ok": False,
                "error": "SSL EOF after submit",
                "ambiguous_acceptance": True,
            }

    telegram = ChurningAmbiguousTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    source_sync._ensure_topic(
        store, worker, entry, runtime, chat_id="-100"
    )
    source_sync._ensure_topic(
        store, worker, entry, runtime, chat_id="-100"
    )

    assert telegram.create_attempts == 1
    assert entry["binding_state"] == "quarantined:ambiguous_topic_create"
    assert any(
        record.get("kind") == "ambiguous_created_topic"
        for record in store["telegram"]["accepted_created_topics"].values()
    )


def test_create_topic_marks_transport_failure_as_ambiguous(monkeypatch):
    def timeout(*_args, **_kwargs):
        raise TimeoutError("response timed out after submit")

    monkeypatch.setattr(
        "herdres_connector.telegram_delivery.urllib.request.urlopen",
        timeout,
    )
    result = TelegramClient(token="test-token").create_topic(
        "test-chat", "Project"
    )

    assert result["ok"] is False
    assert result["ambiguous_acceptance"] is True


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


def test_live_unbound_status_counts_and_marks_only_snapshot_panes():
    html = render_status_overview(
        [
            {
                "agent": "codex",
                "worker_name": "codex",
                "status": "working",
                "live_in_snapshot": True,
                "binding_state": "pending_create",
            },
            {
                "agent": "claude",
                "worker_name": "claude",
                "status": "idle",
                "live_in_snapshot": False,
                "binding_state": "absent_from_snapshot",
            },
        ]
    )

    assert html.splitlines()[0] == "<b>Live panes without topics: 1</b>"
    assert "Codex 🟡 ⚠️ unbound: pending_create" in html
    assert "Claude 🟢 ⚠️" not in html


def test_global_pinned_status_delivers_with_live_unbound_header():
    store = _notification_race_store()
    store["panes"]["worker:unbound"] = {
        "source": "tendwire",
        "entry_type": "worker",
        "tendwire_worker_id": "worker-unbound",
        "worker_id": "worker-unbound",
        "tendwire_space_id": "space-1",
        "space_id": "space-1",
        "status": "working",
        "live_in_snapshot": True,
        "binding_state": "pending_create",
    }
    telegram = FakeTelegram()

    updated = source_sync._sync_pinned(
        store,
        SyncRuntime(
            FakeTendwire(),
            telegram,
            with_outbox=False,
        ),
        chat_id="-100",
    )

    assert updated is True
    assert len(telegram.sent) == 1
    assert telegram.sent[0][2]["thread_id"] == "1"
    assert (
        "<b>Live panes without topics: 1</b>"
        in telegram.sent[0][1]
    )
    assert "⚠️ unbound: pending_create" in telegram.sent[0][1]


def _mixed_status_board_store():
    store = _store()
    _key, routable, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-routable",
                "name": "Worker-routable",
                "space_id": "space-1",
                "fingerprint": "worker-routable-fp",
                "status": "working",
            }
        ),
        topic_id="77",
    )
    routable.update(
        {
            "binding_topic_id": "77",
            "binding_state": "bound",
            "live_in_snapshot": True,
        }
    )
    _key, refused, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-refused",
                "name": "Worker-refused",
                "space_id": "space-1",
                "fingerprint": "worker-refused-fp",
                "status": "working",
            },
            stable_identity=False,
        ),
    )
    refused.update(
        {
            "binding_state": "no_stable_identity",
            "live_in_snapshot": True,
        }
    )
    store["spaces"]["workspace:space-1"] = {
        "source": "tendwire",
        "entry_type": "space",
        "tendwire_space_id": "space-1",
        "space_id": "space-1",
        "topic_id": "77",
        "worker_ids": ["worker-routable"],
        "status": "working",
    }
    return store


def _assert_refused_worker_on_global_board(store):
    telegram = FakeTelegram()
    assert source_sync._sync_pinned(
        store,
        SyncRuntime(
            FakeTendwire(),
            telegram,
            with_outbox=False,
        ),
        chat_id="-100",
    )
    assert len(telegram.sent) == 1
    html = telegram.sent[0][1]
    assert "<b>Live panes without topics: 1</b>" in html
    assert "worker-refused 🟡 ⚠️ unbound: no_stable_identity" in html


def test_space_mode_board_keeps_identity_refused_live_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    store = _mixed_status_board_store()

    _assert_refused_worker_on_global_board(store)

    space = store["spaces"]["workspace:space-1"]
    topic_entries = source_sync._status_entries_for_topic_pin(
        store, space
    )
    assert {
        entry["tendwire_worker_id"] for entry in topic_entries
    } == {"worker-routable", "worker-refused"}


def test_worker_mode_board_ignores_stale_space_worker_allowlist(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _mixed_status_board_store()

    _assert_refused_worker_on_global_board(store)


@pytest.mark.parametrize(
    "binding_state",
    [
        "no_stable_identity",
        "quarantined:snapshot_stable_key_conflict",
        "quarantined:retired_route",
        "quarantined:ambiguous_route",
        "quarantined:stable_identity_collision",
    ],
)
def test_each_binding_refusal_reason_remains_on_global_board(
    binding_state,
):
    store = _mixed_status_board_store()
    refused = next(
        entry
        for entry in state.source_worker_entries(store).values()
        if entry.get("tendwire_worker_id") == "worker-refused"
    )
    refused["binding_state"] = binding_state
    telegram = FakeTelegram()

    assert source_sync._sync_pinned(
        store,
        SyncRuntime(
            FakeTendwire(),
            telegram,
            with_outbox=False,
        ),
        chat_id="-100",
    )

    html = telegram.sent[0][1]
    assert "<b>Live panes without topics: 1</b>" in html
    assert f"⚠️ unbound: {binding_state}" in html


def test_doctor_is_unhealthy_only_for_live_unbound_panes(monkeypatch):
    store = _store()
    historical = {
        "source": "tendwire",
        "entry_type": "worker",
        "live_in_snapshot": False,
        "binding_state": "absent_from_snapshot",
        "tendwire_worker_id": "historical",
    }
    store["panes"]["worker:historical"] = historical
    assert doctor.outbound_unbound_live_panes(store)["ok"] is True

    historical["live_in_snapshot"] = True
    historical["binding_state"] = "no_stable_identity"
    check = doctor.outbound_unbound_live_panes(store)

    assert check["ok"] is False
    assert check["status"] == "live_panes_unbound"
    assert check["unbound_count"] == 1
    assert check["first_unbound"]["worker_id"] == "historical"
    assert check["first_unbound"]["binding_state"] == "no_stable_identity"
    monkeypatch.setattr(
        doctor, "source_services", lambda: {"ok": True}
    )
    monkeypatch.setattr(doctor, "legacy_timer", lambda: {"ok": True})
    monkeypatch.setattr(
        doctor, "tendwire_backend", lambda _client=None: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "tendwire_delta_feed", lambda: {"ok": True}
    )
    monkeypatch.setattr(doctor, "inbound_queue", lambda: {"ok": True})
    monkeypatch.setattr(doctor.state, "load_state", lambda: store)

    composed = doctor.run_doctor()

    assert composed["ok"] is False
    assert (
        composed["checks"]["outbound_unbound_live_panes"]["status"]
        == "live_panes_unbound"
    )


def test_binding_refusals_stamp_each_specific_reason(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)

    no_identity_worker = _source_worker(
        {
            "id": "missing-id",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "missing-id-fp",
        },
        stable_identity=False,
    )
    _key, no_identity, _created = state.upsert_worker_entry(
        store, no_identity_worker
    )
    source_sync._ensure_topic(
        store,
        no_identity_worker,
        no_identity,
        runtime,
        chat_id="-100",
    )
    assert no_identity["binding_state"] == "no_stable_identity"

    quarantined_worker = _source_worker(
        {
            "id": "quarantined",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "quarantined-fp",
        }
    )
    quarantined_key, quarantined, _created = state.upsert_worker_entry(
        store, quarantined_worker
    )
    state.quarantine_worker_entry(
        store,
        quarantined_key,
        reason="snapshot_stable_key_conflict",
    )
    source_sync._ensure_topic(
        store,
        quarantined_worker,
        quarantined,
        runtime,
        chat_id="-100",
    )
    assert quarantined["binding_state"] == (
        "quarantined:snapshot_stable_key_conflict"
    )

    pending_worker = _source_worker(
        {
            "id": "pending",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "pending-fp",
        }
    )
    _key, pending, _created = state.upsert_worker_entry(
        store, pending_worker
    )
    source_sync._ensure_topic(
        store,
        pending_worker,
        pending,
        runtime,
        chat_id="-100",
        can_create=False,
    )
    assert pending["binding_state"] == "pending_create"

    class FailingCreateTelegram(FakeTelegram):
        def create_topic(self, _chat_id, _name, icon_color=None):
            return {"ok": False, "error": "topic quota reached"}

    failed_worker = _source_worker(
        {
            "id": "create-error",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "create-error-fp",
        }
    )
    _key, failed, _created = state.upsert_worker_entry(
        store, failed_worker
    )
    source_sync._ensure_topic(
        store,
        failed_worker,
        failed,
        SyncRuntime(
            FakeTendwire(),
            FailingCreateTelegram(),
            with_outbox=False,
        ),
        chat_id="-100",
    )
    assert failed["binding_state"] == "create_error:topic quota reached"


def test_snapshot_stamps_bound_and_absent_binding_states(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    store = _store()
    historical_worker = _source_worker(
        {
            "id": "historical",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "historical-fp",
        }
    )
    _key, historical, _created = state.upsert_worker_entry(
        store, historical_worker
    )
    historical["topic_id"] = "51"

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    {
                        "id": "live",
                        "name": "Live",
                        "status": "working",
                        "space_id": "space-1",
                        "fingerprint": "live-fp",
                    }
                ]
            ),
            FakeTelegram(),
            with_outbox=False,
        ),
    )
    entries = state.source_worker_entries(store)
    live = next(
        entry
        for entry in entries.values()
        if entry.get("tendwire_worker_id") == "live"
    )

    assert live["live_in_snapshot"] is True
    assert live["binding_state"] == "bound"
    assert live["binding_topic_id"] == live["topic_id"]
    assert historical["live_in_snapshot"] is False
    assert historical["binding_state"] == "absent_from_snapshot"


def test_unique_routing_gate_stamps_ambiguous_route(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setattr(
        state,
        "worker_entry_is_uniquely_routable",
        lambda *_args, **_kwargs: False,
    )
    store = _store()

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(),
            FakeTelegram(),
            with_outbox=False,
        ),
    )
    entry = next(iter(state.source_worker_entries(store).values()))

    assert entry["live_in_snapshot"] is True
    assert entry["binding_state"] == "quarantined:ambiguous_route"
    assert entry.get("topic_id") is None


def test_unbound_final_notice_is_one_per_entry_cooldown(monkeypatch):
    monkeypatch.setenv(
        "HERDRES_UNBOUND_FINAL_NOTICE_COOLDOWN_SECONDS", "300"
    )
    store = _store()
    worker = _source_worker(
        {
            "id": "worker-unbound",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "worker-unbound-fp",
        }
    )
    _key, entry, _created = state.upsert_worker_entry(store, worker)
    entry["live_in_snapshot"] = True
    entry["binding_state"] = "pending_create"
    telegram = FakeTelegram()
    runtime = SyncRuntime(
        FakeTendwire(), telegram, with_outbox=False, max_sends=8
    )
    item = {
        "id": "turn-unbound",
        "worker_id": "worker-unbound",
        "complete": True,
        "assistant_final_text": "owner-visible final",
    }

    first = source_sync._notify_unbound_final(
        store, item, entry, runtime, chat_id="-100"
    )
    second = source_sync._notify_unbound_final(
        store, item, entry, runtime, chat_id="-100"
    )

    assert first == 1
    assert second == 0
    assert len(telegram.sent) == 1
    assert telegram.sent[0][2]["thread_id"] == "1"
    assert "Live pane has no Telegram topic" in telegram.sent[0][1]
    assert entry["unbound_final_notice_turn_id"] == "turn-unbound"


@pytest.mark.parametrize(
    "binding_state",
    [
        "no_stable_identity",
        "quarantined:snapshot_stable_key_conflict",
        "quarantined:ambiguous_route",
    ],
)
def test_identity_resolution_states_do_not_emit_general_final_notices(
    binding_state,
):
    store = _store()
    worker = _source_worker(
        {
            "id": "worker-resolving",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "worker-resolving-fp",
        }
    )
    _key, entry, _created = state.upsert_worker_entry(store, worker)
    entry["live_in_snapshot"] = True
    entry["binding_state"] = binding_state
    telegram = FakeTelegram()

    writes = source_sync._notify_unbound_final(
        store,
        {
            "id": "turn-resolving",
            "worker_id": "worker-resolving",
            "complete": True,
            "assistant_final_text": "waiting for identity resolution",
        },
        entry,
        SyncRuntime(
            FakeTendwire(),
            telegram,
            with_outbox=False,
            max_sends=8,
        ),
        chat_id="-100",
    )

    assert writes == 0
    assert telegram.sent == []


def test_identity_resolution_that_never_heals_stays_board_and_doctor_visible():
    store = _store()
    entry = {
        "source": "tendwire",
        "entry_type": "worker",
        "agent": "claude",
        "worker_name": "claude",
        "tendwire_worker_id": "worker-stranded",
        "live_in_snapshot": True,
        "binding_state": "quarantined:snapshot_stable_key_conflict",
        "status": "working",
    }
    store["panes"]["worker:stranded"] = entry

    # Identity-resolution states deliberately never emit a General message.
    # If consolidation does not heal, the existing owner surfaces remain
    # continuously non-healthy instead of flooding once per claimant/pass.
    for _pass in range(4):
        overview = render_status_overview(
            list(state.source_worker_entries(store).values())
        )
        check = doctor.outbound_unbound_live_panes(store)
        assert overview.splitlines()[0] == (
            "<b>Live panes without topics: 1</b>"
        )
        assert (
            "Claude 🟡 ⚠️ unbound: "
            "quarantined:snapshot_stable_key_conflict"
        ) in overview
        assert check["ok"] is False
        assert check["status"] == "live_panes_unbound"
        assert check["first_unbound"]["worker_id"] == "worker-stranded"


def test_unbound_final_notice_runs_on_sync_drop_path():
    turns = {
        "schema_version": 1,
        "turns": [
            {
                "id": "turn-pending-topic-final",
                "worker_id": "worker-1",
                "space_id": "space-1",
                "complete": True,
                "assistant_final_text": "final cannot reach a pane topic",
            }
        ]
    }
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
    )
    entry.update(
        {
            "live_in_snapshot": True,
            "binding_state": "pending_create",
            "status": "working",
        }
    )
    telegram = FakeTelegram()
    runtime = SyncRuntime(
        FakeTendwire(),
        telegram,
        with_outbox=False,
        max_sends=8,
    )

    first = source_sync._sync_turns(
        store,
        turns,
        {"pending_interactions": []},
        runtime,
        chat_id="-100",
        live_worker_ids={"worker-1"},
    )
    second = source_sync._sync_turns(
        store,
        turns,
        {"pending_interactions": []},
        runtime,
        chat_id="-100",
        live_worker_ids={"worker-1"},
    )

    assert first["feed_sent"] == 0
    assert first["work_pending"] == 1
    assert first["physical_writes"] == 1
    assert len(telegram.sent) == 1
    assert "Live pane has no Telegram topic" in telegram.sent[0][1]
    assert second["feed_sent"] == 0
    assert second["work_pending"] == 1
    assert second["physical_writes"] == 0
    assert len(telegram.sent) == 1
    assert entry["binding_state"] == "pending_create"






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
    assert "✅ <b>Response" not in html
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
    assert "✅ <b>Response" in html
    assert "Final text from a completed stream-only turn." in html




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
    monkeypatch.setenv("HERDRES_TENDWIRE_FORCE_FULL_RECONCILE", "1")
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
        "required_turn_schema_version": 2,
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

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                turns={
                    "turns": [
                        {
                            "id": "turn-1",
                            "worker_id": "worker-1",
                            "assistant_final_text": "already delivered",
                            "complete": True,
                        }
                    ]
                }
            ),
            FakeTelegram(),
            with_outbox=False,
        ),
    )

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
    assert len(checkpoints) == 2
    assert checkpoints[0]["telegram"]["accepted_created_topics"]
    persisted_entry = next(
        iter(state.source_space_entries(checkpoints[-1]).values())
    )
    assert persisted_entry["topic_id"] == entry["topic_id"]
    assert not checkpoints[-1]["telegram"].get(
        "accepted_created_topics"
    )

    restored = checkpoints[-1]
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


def test_topic_create_acceptance_crash_restarts_from_compact_receipt(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    source = {
        "id": "space-acceptance-crash",
        "name": "Acceptance crash",
        "status": "active",
        "fingerprint": "space-acceptance-crash-fp",
    }
    _key, entry, _created = state.upsert_space_entry(store, source)
    telegram = FakeTelegram()
    checkpoints = []

    def crash_at_first_barrier():
        checkpoints.append(copy.deepcopy(store))
        raise RuntimeError("crash after topic acceptance")

    runtime = SyncRuntime(
        FakeTendwire(),
        telegram,
        with_outbox=False,
        checkpoint=crash_at_first_barrier,
    )
    with pytest.raises(
        RuntimeError, match="crash after topic acceptance"
    ):
        source_sync._ensure_topic(
            store,
            source,
            entry,
            runtime,
            chat_id="-100",
        )

    assert telegram.topics == ["Acceptance crash"]
    persisted = checkpoints[-1]
    assert persisted["telegram"]["accepted_created_topics"]
    persisted_entry = next(
        iter(state.source_space_entries(persisted).values())
    )
    assert not persisted_entry.get("topic_id")
    resumed_runtime = SyncRuntime(
        FakeTendwire(), telegram, with_outbox=False
    )

    assert source_sync._recover_accepted_created_topics(
        persisted, resumed_runtime
    ) == 1
    persisted_entry = next(
        iter(state.source_space_entries(persisted).values())
    )
    assert persisted_entry["topic_id"] == "77"
    needed, created = source_sync._ensure_topic(
        persisted,
        source,
        persisted_entry,
        resumed_runtime,
        chat_id="-100",
    )
    assert needed is False and created is False
    assert telegram.topics == ["Acceptance crash"]
    assert not persisted["telegram"].get("accepted_created_topics")


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
    assert second["binding_state"] == (
        "quarantined:stable_identity_collision"
    )
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
    # No redundant top worker title; the newest Response details block is open.
    assert "<h3>Alpha</h3>" not in html
    assert html.startswith(
        "<details open><summary>✅ <b>Response</b></summary>"
    )
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

    assert html.startswith(
        "<details open><summary>✅ <b>Response</b></summary>"
    )
    assert "<blockquote>" not in html
    assert "<blockquote expandable>" not in html
    assert "##" not in html
    assert "**" not in html
    assert "<h4>Plan</h4>" in html
    assert "<ul>" in html
    assert "<li>keep <b>rich</b> sections</li>" in html
    assert "</details><br><details" not in html








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
    # delivered as a SINGLE open Response details block -- no "1/N" split.
    parts = render_feed_item_delivery_html_parts(
        {"kind": "turn", "assistant_final_text": _recent_cutoff_response_text()}
    )

    assert len(parts) == 1
    assert parts[0].startswith(
        "<details open><summary>✅ <b>Response</b></summary>"
    )
    assert "4557d20 Prevent child bot target races" in parts[0]
    assert "branch: <code>tendwired</code>" in parts[0]




def test_oversize_response_splits_losslessly_into_labeled_parts():
    # A response too large for one rich message still splits, losslessly, into
    # labeled "Response i/N" parts -- each under the per-message cap.
    tail = "TAIL_MARKER_LOSSLESS"
    text = "## **Long**\n\n" + ("- keep **rich** sections\n" * 950) + tail
    parts = render_feed_item_delivery_html_parts({"kind": "turn", "assistant_final_text": text})

    assert len(render_turn_item_html({"kind": "turn", "assistant_final_text": text})) > MAX_RICH_HTML_CHARS
    assert len(parts) > 1
    total = len(parts)
    for index, part in enumerate(parts, start=1):
        assert part.startswith(
            "<details open><summary>✅ "
            f"<b>Response {index}/{total}</b></summary>"
        )
        assert len(part) <= MAX_RICH_HTML_CHARS
    combined = "\n".join(parts)
    assert tail in combined                       # nothing cut
    assert combined.count("<summary>✅ <b>Response ") == total




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
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def send_message(self, chat_id, html, **kwargs):
            self.attempts += 1
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

    result = drain_outbox(
        store, telegram, tendwire, chat_id="-100", max_sends=2
    )

    assert result["delivered"] == 1
    assert result["acked"] == 1
    assert result["failed"] == 0
    assert result["physical_writes"] == 2
    assert telegram.attempts == 2
    assert tendwire.failed == []
    assert tendwire.acked == [("ref-1", {"telegram": "delivered"})]
    assert telegram.sent[-1][2].get("thread_id") is None
    assert store["telegram_dead_topic_ids"] == ["1"]
    assert "topic_id" not in stale_alias
    assert stale_alias["deleted_topic_id"] == "1"


def test_outbox_topic_fallback_obeys_exact_physical_write_allowance():
    class OutboxTendwire:
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
                            "attention": {
                                "severity": "warning",
                                "reason": "Needs input",
                            },
                        },
                    }
                ],
            }

        def connector_fail(self, _ref, _error, **_kwargs):
            return {"ok": True}

    class TopicMissingTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def send_message(self, chat_id, html, **kwargs):
            self.attempts += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }

    store = _store()
    telegram = TopicMissingTelegram()

    result = drain_outbox(
        store,
        telegram,
        OutboxTendwire(),
        chat_id="-100",
        max_sends=1,
    )

    assert result["delivered"] == 0
    assert result["failed"] == 1
    assert result["physical_writes"] == 1
    assert telegram.attempts == 1


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












def test_delivered_turn_ledger_keeps_more_than_old_1000_limit():
    store = {}
    for index in range(1001):
        state.mark_delivered(store, f"final:turn-{index}:hash", {"turn_id": f"turn-{index}"})

    ledger = store["tendwire_source_delivered_turns"]
    assert len(ledger) == 1001
    assert "final:turn-0:hash" in ledger










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


def test_space_topic_pin_loop_skips_historical_worker_rows(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    store = _store()
    store["spaces"] = {
        "workspace:live": {
            "source": "tendwire",
            "entry_type": "space",
            "topic_name": "Live",
            "topic_id": "77",
            "status": "failed",
        }
    }
    store["panes"] = {
        f"worker:historical-{index}": {
            "source": "tendwire",
            "entry_type": "worker",
            "topic_name": f"Historical {index}",
            "topic_id": str(100 + index),
            "status": "idle",
            "live_in_snapshot": False,
        }
        for index in range(100)
    }
    calls = []
    yields = []

    def record(_store, entry, _runtime, **_kwargs):
        calls.append(entry["topic_name"])
        return True

    monkeypatch.setattr(source_sync, "_sync_topic_pinned", record)
    updated = source_sync._sync_topic_pinned_statuses(
        store,
        SyncRuntime(FakeTendwire(), FakeTelegram(), with_outbox=False),
        chat_id="-100",
        yield_barrier=lambda: yields.append(True),
    )

    assert updated == 1
    assert calls == ["Live"]
    assert yields == [True]


def test_worker_topic_pin_loop_skips_space_and_historical_rows(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    store["spaces"] = {
        "workspace:historical": {
            "source": "tendwire",
            "entry_type": "space",
            "topic_name": "Historical space",
            "topic_id": "77",
            "status": "idle",
        }
    }
    store["panes"] = {
        "worker:live": {
            "source": "tendwire",
            "entry_type": "worker",
            "topic_name": "Live worker",
            "topic_id": "88",
            "status": "idle",
            "live_in_snapshot": True,
        },
        "worker:live-failed": {
            "source": "tendwire",
            "entry_type": "worker",
            "topic_name": "Live failed worker",
            "topic_id": "89",
            "status": "failed",
            "live_in_snapshot": True,
        },
        "worker:historical": {
            "source": "tendwire",
            "entry_type": "worker",
            "topic_name": "Historical worker",
            "topic_id": "99",
            "status": "idle",
            "live_in_snapshot": False,
        },
    }
    calls = []
    yields = []

    def record(_store, entry, _runtime, **_kwargs):
        calls.append(entry["topic_name"])
        return True

    monkeypatch.setattr(source_sync, "_sync_topic_pinned", record)
    updated = source_sync._sync_topic_pinned_statuses(
        store,
        SyncRuntime(FakeTendwire(), FakeTelegram(), with_outbox=False),
        chat_id="-100",
        yield_barrier=lambda: yields.append(True),
    )

    assert updated == 2
    assert calls == ["Live worker", "Live failed worker"]
    assert yields == [True, True]




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






def test_invalid_snapshot_clears_stale_space_route_and_reply_binding(
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
        "meta": {
            "stable_key": "public-stable-key",
            "plan": "Migration roadmap",
            "thought": "Product-design note",
        },
        "_meta": {"adapter/internal": "private-extension"},
        "agent_event": {
            "thought": "private thought",
            "reasoning": "private reasoning",
            "rawInput": {"command": "private input"},
            "raw_output": "private output",
        },
    }
    clean = public_prune(payload)
    encoded = json.dumps(clean)
    assert "-100" not in encoded
    assert "secret" not in encoded
    assert "backend_target" not in encoded
    assert "private-extension" not in encoded
    assert "private thought" not in encoded
    assert "private reasoning" not in encoded
    assert "private input" not in encoded
    assert "private output" not in encoded
    assert clean["meta"] == {
        "stable_key": "public-stable-key",
        "plan": "Migration roadmap",
        "thought": "Product-design note",
    }


def test_public_prune_drops_acp_discriminated_event_with_sibling_content():
    private = "internal chain of thought that must stay private"
    payload = {
        "ok": True,
        "update": {
            "sessionUpdate": "agent_thought_chunk",
            "content": {"type": "text", "text": private},
        },
        "public": "still visible",
    }

    clean = public_prune(payload)

    assert clean == {
        "ok": True,
        "update": {},
        "public": "still visible",
    }
    assert private not in json.dumps(clean)


@pytest.mark.parametrize(
    "private_payload",
    [
        {
            "sessionUpdate": "plan_removed",
            "planId": "private-plan-id",
        },
        {
            "jsonrpc": "2.0",
            "method": "session/request_permission",
            "params": {
                "sessionId": "private-session-id",
                "options": [{"optionId": "private-option-id"}],
            },
        },
    ],
)
def test_public_prune_drops_current_acp_envelopes(private_payload):
    clean = public_prune({"public": "safe", "private": private_payload})

    assert clean == {"public": "safe", "private": {}}
    assert "private-" not in json.dumps(clean)


def test_public_prune_removes_normalized_raw_agent_identities():
    payload = {
        "public": "safe",
        "Session-ID": "private-session-id",
        "sourceSessionId": "private-source-session-id",
        "tool_call_id": "private-tool-id",
        "toolUseId": "private-tool-use-id",
    }

    assert public_prune(payload) == {"public": "safe"}


def test_outbox_rejects_unversioned_structured_agent_event_without_telegram_send():
    class StructuredEventTendwire:
        def __init__(self):
            self.failed = []

        def connector_poll(self, **_kwargs):
            return {
                "ok": True,
                "items": [
                    {
                        "ref": "ref-private-event",
                        "key": "agent-event:tool-1",
                        "attempt": 1,
                        "payload": {
                            "event_type": "tool_call_update",
                            "tool_call": {
                                "title": "Shell",
                                "rawInput": "private command",
                                "rawOutput": "private output",
                            },
                            "_meta": {"adapter/private": "private metadata"},
                        },
                    }
                ],
            }

        def connector_fail(self, ref, error, **_kwargs):
            self.failed.append((ref, error))
            return {"ok": True}

    store = _store()
    tendwire = StructuredEventTendwire()
    telegram = FakeTelegram()

    result = drain_outbox(
        store, telegram, tendwire, chat_id="-100", max_sends=1
    )

    assert result["delivered"] == 0
    assert result["failed"] == 1
    assert result["physical_writes"] == 0
    assert telegram.sent == []
    assert tendwire.failed == [
        ("ref-private-event", "unsupported connector event type")
    ]


def test_status_and_pending_render_only_neutral_public_fields():
    private = "must-not-reach-telegram"
    entry = {
        "worker_name": "Codex",
        "tendwire_worker_id": "worker-1",
        "status": "working",
        "_meta": {"adapter/private": private},
        "control": {"currentMode": private},
    }
    pending = {
        "question": "Allow this operation?",
        "choices": [{"label": "Allow once"}, {"label": "Reject"}],
        "permission": {"toolCall": {"rawInput": private}},
        "_meta": {"adapter/private": private},
    }

    status_html = render_status_overview([entry])
    pending_html = render_pending(pending, entry)

    assert "Codex" in status_html
    assert "Allow this operation?" in pending_html
    assert "Allow once" in pending_html
    assert private not in status_html
    assert private not in pending_html






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
