# TrashPanda Operational Workflow

> **Bitácora de despliegue real:** ver
> [`PRODUCTION_BOOTSTRAP_LOG.md`](PRODUCTION_BOOTSTRAP_LOG.md) para
> el registro paso a paso del primer arranque productivo contra
> `fuenmayorindustries.com` (DNS records SPF/DKIM/DMARC, App
> Password de Google Workspace, override systemd con la IMAP password,
> verificación de port 25 outbound).
>
> **Procedimiento del pilot send:** ver
> [`PILOT_RUNBOOK.md`](PILOT_RUNBOOK.md) — pre-flight checklist,
> endpoints exactos para config / preview / launch / poll / finalize,
> métricas a monitorear a las 24/48/72h, criterios verde/amarillo/rojo
> de continuación, y protocolos de pause / abort.

## 1. Setup inicial del VPS (una sola vez):

```bash
ssh root@192.3.105.145
curl -fsSL https://raw.githubusercontent.com/AlexanderAtlasLuthor/TrashPanda/main/deploy/install_vps.sh -o install.sh
bash install.sh
# → te imprime el TRASHPANDA_OPERATOR_TOKEN
```

Esto instala y arranca **todo el stack del VPS** de un solo golpe:

| Unit | Tipo | Cadencia |
|------|------|----------|
| `trashpanda-backend.service` | servicio long-running | siempre activo |
| `trashpanda-retry-worker.timer` | timer → oneshot | cada 15 min |
| `trashpanda-pilot-bounce-poller.timer` | timer → oneshot | (ver `.timer`) |

Los timers quedan habilitados al boot, así que tras un reinicio del VPS
no hay que tocar nada. Verifica con:

```bash
systemctl list-timers --no-pager 'trashpanda-*'
```

Para actualizar a la última versión del repo (re-instala las units si
cambiaron + reinicia el backend):

```bash
bash /root/trashpanda/deploy/update_vps.sh
```

## 2. Setup túnel en laptop (una sola vez si usas systemd-user, o cada arranque si usas PS1):

### macOS/Linux con systemd (sobrevive logout, autossh-aware):
```bash
mkdir -p ~/.config/systemd/user && cp deploy/trashpanda-tunnel.service ~/.config/systemd/user/
systemctl --user enable --now trashpanda-tunnel
```

### Windows:
```powershell
.\deploy\tunnel.ps1
```

## 3. One-click en Windows (recomendado):

### Setup automático (una sola vez)

Si nunca configuraste esta laptop, corre el bootstrap que hace los
4 pasos de prerequisitos en orden — instala OpenSSH si falta,
intercambia la llave SSH con el VPS, escribe `trashpanda-next/.env.local`
con el token correcto, y crea el ícono de escritorio:

```powershell
.\deploy\setup_windows.ps1
# o, si tu IP del VPS no es la default:
.\deploy\setup_windows.ps1 -VpsHost root@TU.IP.DEL.VPS
```

Te pedirá la contraseña del VPS **una sola vez** (durante el paso de
SSH). El resto es automático. Es idempotente — re-correrlo en una
laptop que ya está lista no duplica nada.

### Uso diario

Doble-click al ícono **"TrashPanda (VPS)"** del escritorio. Se abren
dos ventanas:

- *TrashPanda - Tunnel* — supervisor SSH (auto-reconecta si cae).
- *TrashPanda - Frontend* — Next.js dev server.

Cuando 3000 responde, se abre `http://localhost:3000` en el browser.
Para parar todo: `stop_vps.bat` o cierra ambas ventanas.

> **Nota:** el ícono `TrashPanda` (sin sufijo) sigue arrancando el modo
> dev local (FastAPI + Next.js en tu laptop). El nuevo `TrashPanda
> (VPS)` apunta al backend del servidor remoto vía túnel SSH.

Si prefieres hacer los 4 pasos manualmente (en vez de
`setup_windows.ps1`), aquí está el detalle:

- `ssh.exe` en PATH (Windows Settings → Apps → Optional Features → OpenSSH Client).
- Llave SSH al VPS: `.\deploy\setup_ssh_key.ps1`.
- Token en `trashpanda-next\.env.local`:
  ```
  TRASHPANDA_OPERATOR_TOKEN=<el que imprimió install_vps.sh>
  TRASHPANDA_BACKEND_URL=http://localhost:8001
  ```
- Ícono de escritorio: `.\scripts\create_shortcut_vps.ps1`.