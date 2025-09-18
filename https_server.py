
"""
https_server.py – avvia l'applicazione Flask su HTTPS in modo robusto.

Questo modulo espone l'istanza ``app`` definita in ``app.py`` utilizzando
un server WSGI multi‑thread basato su ``wsgiref`` con supporto TLS/SSL.

La versione originale di questo script utilizzava ``app.run`` con
``ssl_context``. Il server di sviluppo integrato di Flask non è
progettato per l'uso prolungato e in presenza di molte connessioni
può esaurire risorse o andare in deadlock dopo alcune ore o giorni.

Per ovviare a questi problemi il server qui definito sfrutta
``wsgiref.simple_server`` combinato con ``socketserver.ThreadingMixIn``
per gestire ogni richiesta in un thread separato. La comunicazione
HTTPS è abilitata avvolgendo il socket del server con un contesto SSL
creato tramite il modulo ``ssl`` della libreria standard.  Tale
approccio mantiene la compatibilità con l'applicazione e garantisce
maggiore affidabilità rispetto al server di sviluppo, pur restando
relativamente leggero e privo di dipendenze esterne.

Se non esistono certificato e chiave personalizzati verranno generati
automaticamente certificati self‑signed tramite ``openssl``. Puoi
sostituire ``cert.pem`` e ``key.pem`` con certificati firmati da una
CA riconosciuta se necessario.
"""

from __future__ import annotations

import os
import ssl
from wsgiref.simple_server import make_server, WSGIRequestHandler, WSGIServer
from socketserver import ThreadingMixIn
from typing import Iterable

from app import app


class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    """A WSGI server that handles each request in a separate thread.

    ThreadingMixIn provides a thread per request.  Setting
    ``daemon_threads = True`` ensures that worker threads do not block
    shutdown of the main process if the server is stopped.
    """
    daemon_threads = True


def ensure_certificate(cert_file: str = "cert.pem", key_file: str = "key.pem") -> None:
    """Ensure that a certificate/key pair exists.

    If either the certificate or the key does not exist, a self‑signed
    certificate is generated using ``openssl``.  The generated
    certificate will be valid for 365 days and is sufficient for
    development and testing purposes.  For production use consider
    replacing these files with a certificate signed by a trusted CA.

    :param cert_file: path to the PEM formatted certificate
    :param key_file: path to the PEM formatted private key
    """
    if not os.path.exists(cert_file) or not os.path.exists(key_file):
        print("Generazione certificato self‑signed per HTTPS…")
        # Generate a self‑signed certificate using openssl.  The
        # ``-subj`` argument defines the certificate's subject fields.
        subj = "/C=IT/ST=Italy/L=Magazzino/O=TondiMeccanica/OU=IT/CN=Magazzino"
        cmd = (
            f"openssl req -newkey rsa:2048 -x509 -sha256 -days 365 -nodes "
            f"-out {cert_file} -keyout {key_file} -subj '{subj}'"
        )
        os.system(cmd)


def run_server(host: str = "0.0.0.0", port: int = 5000, cert_file: str = "cert.pem", key_file: str = "key.pem") -> None:
    """Run the Flask application over HTTPS using a threaded WSGI server.

    This function creates a WSGI server that listens on the given host
    and port, wraps its socket with SSL, and serves requests
    indefinitely.  Each incoming connection is handled in its own
    thread.  Press Ctrl+C to stop the server.

    :param host: hostname or IP address to bind (default 0.0.0.0)
    :param port: TCP port to bind (default 5000)
    :param cert_file: path to the SSL certificate file in PEM format
    :param key_file: path to the SSL private key file in PEM format
    """
    # Make sure the certificate and key exist (generate if missing)
    ensure_certificate(cert_file, key_file)
    # Create the WSGI server instance with threading support
    httpd = make_server(host, port, app, server_class=ThreadedWSGIServer, handler_class=WSGIRequestHandler)
    # Configure SSL context and wrap the underlying socket
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    # Determine and display reachable URLs for the user
    try:
        # If the host is 0.0.0.0, attempt to determine local IPv4 addresses
        addrs: Iterable[str]
        if host == "0.0.0.0":
            # Try reading IP addresses from the system's network interfaces
            # Fallback to a static list if detection fails
            addrs = []
            try:
                import netifaces  # type: ignore
                for iface in netifaces.interfaces():
                    ifaddrs = netifaces.ifaddresses(iface).get(netifaces.AF_INET, [])
                    for addr_info in ifaddrs:
                        ip = addr_info.get('addr')
                        if ip and ip != '127.0.0.1':
                            addrs.append(ip)
            except Exception:
                # netifaces may not be installed; use a predefined list or
                # skip detection.  It's better to print at least the
                # generic host.
                addrs = []
            if not addrs:
                addrs = [host]
        else:
            addrs = [host]
        print("Server HTTPS pronto su:")
        for ip in addrs:
            print(f"  https://{ip}:{port}")
        # Serve requests until interrupted
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nArresto del server richiesto dall'utente.")


if __name__ == "__main__":
    run_server()
