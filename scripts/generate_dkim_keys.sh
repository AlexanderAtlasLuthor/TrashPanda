#!/usr/bin/env bash
# V2.10.13 — DKIM key generator for the pilot-send sender domain.
#
# Generates an RSA-2048 keypair and prints the DNS TXT record that
# the operator pastes into the sender domain's authoritative DNS,
# plus the private key path the MTA / OpenDKIM signer needs.
#
# Usage:
#   scripts/generate_dkim_keys.sh <selector> <domain> [out_dir]
#
# Example:
#   scripts/generate_dkim_keys.sh tp acme.com ./dkim
#
# Emits:
#   <out_dir>/<selector>.<domain>.private  (mode 0600 — feed to MTA)
#   <out_dir>/<selector>.<domain>.public   (PEM, for reference)
#   <out_dir>/<selector>.<domain>.txt      (DNS TXT record body)
#
# DNS hostname:
#   <selector>._domainkey.<domain>
#
# Requires: openssl. No network calls.

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <selector> <domain> [out_dir]" >&2
    echo "  e.g. $0 tp acme.com ./dkim" >&2
    exit 2
fi

selector=$1
domain=$2
out_dir=${3:-./dkim}

if ! command -v openssl >/dev/null 2>&1; then
    echo "error: openssl is not installed" >&2
    exit 3
fi

# Validate selector + domain shape early — DKIM selectors are
# label-only (no dots), domains must look like a real FQDN.
if ! [[ $selector =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]]; then
    echo "error: selector '$selector' is not a valid DNS label" >&2
    exit 4
fi
if ! [[ $domain =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]]; then
    echo "error: domain '$domain' does not look like a valid FQDN" >&2
    exit 4
fi

mkdir -p "$out_dir"

priv="$out_dir/$selector.$domain.private"
pub="$out_dir/$selector.$domain.public"
txt="$out_dir/$selector.$domain.txt"

if [[ -f $priv ]]; then
    echo "error: $priv already exists; refusing to overwrite" >&2
    exit 5
fi

# Private key — 2048-bit RSA is the DKIM standard. Larger keys
# overflow a single TXT record (255-byte limit per string) without
# splitting; 2048 fits cleanly when base64-encoded.
openssl genrsa -out "$priv" 2048 2>/dev/null
chmod 600 "$priv"

# Public key in PEM (kept for reference) and base64 single-line for
# the TXT record.
openssl rsa -in "$priv" -pubout -out "$pub" 2>/dev/null

pubkey_b64=$(
    openssl rsa -in "$priv" -pubout -outform DER 2>/dev/null \
        | openssl base64 -A
)

# DKIM TXT format: v=DKIM1; k=rsa; p=<base64-key>
record="v=DKIM1; k=rsa; p=$pubkey_b64"
printf '%s\n' "$record" > "$txt"

cat <<EOF

DKIM keypair generated.

Private key  : $priv  (mode 0600 — install in your MTA / OpenDKIM)
Public key   : $pub
TXT record   : $txt

Add this DNS record at the domain registrar:

  Hostname : ${selector}._domainkey.${domain}
  Type     : TXT
  Value    : ${record}

After publishing, verify with:

  dig +short TXT ${selector}._domainkey.${domain}

Reminder: DKIM only takes effect when the signing MTA actually uses
this private key. Direct-to-MX without an MTA does NOT sign DKIM.

EOF
