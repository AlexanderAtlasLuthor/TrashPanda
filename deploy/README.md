# TrashPanda Operational Workflow

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

Si prefieres no tener que correr el túnel a mano, hay un launcher de
un solo click que (a) levanta el túnel SSH supervisado, (b) arranca
Next.js apuntando a `http://localhost:8001`, (c) abre el browser:

```powershell
.\scripts\create_shortcut_vps.ps1   # una sola vez — crea el ícono
```

Luego doble-click al ícono **"TrashPanda (VPS)"** del escritorio. Se
abrirán dos ventanas:

- *TrashPanda - Tunnel* — supervisor SSH (auto-reconecta si cae).
- *TrashPanda - Frontend* — Next.js dev server.

Para parar todo: `stop_vps.bat` o cierra ambas ventanas.

> **Nota:** el ícono `TrashPanda` (sin sufijo) sigue arrancando el modo
> dev local (FastAPI + Next.js en tu laptop). El nuevo `TrashPanda
> (VPS)` apunta al backend del servidor remoto vía túnel SSH.

Pre-requisitos para el ícono VPS:

- `ssh.exe` en PATH (Windows Settings → Apps → Optional Features → OpenSSH Client).
- Llave SSH ya autorizada en el VPS (ver `deploy/setup_ssh_key.ps1`).
- `trashpanda-next/.env.local` con `TRASHPANDA_OPERATOR_TOKEN` (el launcher
  fuerza `TRASHPANDA_BACKEND_URL=http://localhost:8001` para esta sesión).