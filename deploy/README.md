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