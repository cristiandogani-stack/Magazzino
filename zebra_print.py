"""Utility functions for printing labels on a Zebra printer.

This module encapsulates the logic required to send ZPL (Zebra
Programming Language) commands to a network‑connected Zebra label
printer.  It reads connection settings such as the printer IP address
and port from an external configuration file (``zebra_config.json``)
located in the same directory.  A simple helper function
``print_labels()`` is provided to print one or more QR code labels.

Each label consists of a QR code encoding the material ID and the ID
printed in large font underneath.  The ZPL used here was kept
deliberately minimal so that it can be customised easily in the
configuration file or by modifying the ``generate_zpl()`` function.

Usage example::

    from zebra_print import print_labels
    print_labels([123, 456])

This will print two labels: one for ID ``123`` and one for ID
``456``.  The printer host and port will be read from
``zebra_config.json``.  If the configuration file does not exist or
is malformed, default values (``192.168.100.51:9100``) will be used.

Note: any network errors during printing are silently ignored.  In a
production environment it may be desirable to handle exceptions and
report failures back to the caller.
"""

from __future__ import annotations

import json
import os
import socket
from typing import Iterable, List

# Default connection parameters in case the configuration file is
# missing or cannot be parsed.  The defaults mirror the settings used
# in the original ``zebra_stampa.py`` script.
DEFAULT_CONFIG = {
    "host": "192.168.100.51",
    "port": 9100,
    # The following parameters influence the generated ZPL.  You can
    # adjust the QR code size and text positioning here without
    # modifying the code.  ``qr_module_size`` controls the size of the
    # QR code (higher means larger), and ``text_y_offset`` specifies
    # the vertical position of the ID text relative to the top of the
    # label.
    "qr_module_size": 5,
    "text_y_offset": 230,
    "font_height": 50,
    "font_width": 50,
}

# Name of the JSON configuration file expected to live next to this
# module.  Users can override the printer address and label
# parameters by editing this file.
CONFIG_FILENAME = "zebra_config.json"


def _load_config() -> dict:
    """Load the Zebra printer configuration from JSON.

    The configuration file must be located in the same directory as
    this module.  If the file cannot be loaded or parsed, the
    ``DEFAULT_CONFIG`` is returned.  Only known keys are applied;
    unknown keys are ignored.

    :return: a dictionary with configuration parameters
    """
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILENAME)
    cfg: dict = DEFAULT_CONFIG.copy()
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Apply only known keys to avoid injecting unexpected settings
        for key in DEFAULT_CONFIG:
            if key in data:
                cfg[key] = data[key]
    except Exception:
        # If the file doesn't exist or is invalid, fall back to defaults.
        pass
    return cfg


def _generate_zpl(material_id: int, cfg: dict) -> str:
    """Generate a ZPL string for a single material ID.

    The returned ZPL will print a QR code encoding the material ID
    followed by the same ID printed as plain text.  The size and
    positions are controlled via the configuration passed in.

    :param material_id: the numeric identifier to encode and print
    :param cfg: configuration dictionary controlling the output
    :return: a ZPL string
    """
    # Extract settings with sensible defaults
    qr_size = int(cfg.get("qr_module_size", DEFAULT_CONFIG["qr_module_size"]))
    text_y = int(cfg.get("text_y_offset", DEFAULT_CONFIG["text_y_offset"]))
    font_height = int(cfg.get("font_height", DEFAULT_CONFIG["font_height"]))
    font_width = int(cfg.get("font_width", DEFAULT_CONFIG["font_width"]))
    # ZPL commands:
    # ^XA     - start a label format
    # ^BQN    - print a QR code (orientation N, model 2)
    # ^FDQA, - Field Data with input mode ``QA`` (automatic selection of
    #          QR code version) followed by the encoded data.  See
    #          ZPL II documentation for details.
    # ^FOx,y  - Field origin: positions the next field on the label.
    # ^A0N    - Selects the font (font 0, normal orientation) with
    #           specified height and width.
    # ^XZ     - end of label format
    zpl_lines = ["^XA"]
    # Place the QR code near the top-left of the label
    zpl_lines.append(f"^FO50,50^BQN,2,{qr_size}^FDQA,{material_id}^FS")
    # Print the ID as text beneath the QR code
    zpl_lines.append(f"^FO50,{text_y}^A0N,{font_height},{font_width}^FDID {material_id}^FS")
    zpl_lines.append("^XZ")
    return "\n".join(zpl_lines)


def _send_zpl(zpl: str, host: str, port: int) -> None:
    """Send a ZPL string to the Zebra printer via TCP.

    Opens a socket connection to the specified host and port, sends
    the ZPL string encoded as ASCII and then closes the connection.  In
    case of any socket errors the exception is suppressed to avoid
    crashing the caller.

    :param zpl: the complete ZPL code to send
    :param host: printer hostname or IP address
    :param port: printer port (typically 9100)
    """
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            sock.sendall(zpl.encode("ascii"))
    except Exception:
        # Ignore network errors; in a production system you may want to
        # log or re‑raise these exceptions.
        pass


def print_labels(ids: Iterable[int]) -> None:
    """Print one or more labels for the given material IDs.

    For each ID in ``ids`` a separate ZPL template is generated and
    sent to the Zebra printer.  The printer connection settings and
    label layout options are read from ``zebra_config.json``.

    :param ids: an iterable of integer material identifiers
    """
    cfg = _load_config()
    host: str = cfg.get("host", DEFAULT_CONFIG["host"])
    port: int = int(cfg.get("port", DEFAULT_CONFIG["port"]))
    for material_id in ids:
        try:
            material_int = int(material_id)
        except (ValueError, TypeError):
            # Skip values that cannot be converted to an integer ID
            continue
        zpl = _generate_zpl(material_int, cfg)
        _send_zpl(zpl, host, port)