# TrashPanda Operational Workflow

## 1. Setup inicial del VPS (una sola vez):

```bash
ssh root@192.3.105.145
curl -fsSL https://raw.githubusercontent.com/AlexanderAtlasLuthor/TrashPanda/main/deploy/install_vps.sh -o install.sh
bash install.sh
# → te imprime el TRASHPANDA_OPERATOR_TOKEN
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