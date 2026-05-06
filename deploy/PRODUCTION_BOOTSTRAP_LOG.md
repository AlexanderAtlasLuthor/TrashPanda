# TrashPanda — Bitácora de despliegue a producción

**Fecha:** 2026-05-06
**Dominio operator:** `fuenmayorindustries.com`
**VPS:** `192.3.105.145` (RackNerd)
**Versión TrashPanda:** V2.10.12 + V2.10.13 (relay path opcional)

Esta es la bitácora del primer arranque real del stack TrashPanda
contra un dominio de envío productivo. Sirve como referencia para
re-deployar a otro dominio o para diagnosticar el estado actual.

---

## 1. Configuración del correo del operator

Definimos que el correo de negocio/operator para TrashPanda será:

```
team@fuenmayorindustries.com
```

Como usamos Google Workspace, activamos la verificación en 2 pasos
para poder generar una **App Password** de Google.

Esa App Password se usó para que el bounce poller pueda entrar al
inbox por IMAP y leer rebotes.

## 2. Conexión al VPS

Conexión SSH al VPS:

```
IP:      192.3.105.145
Usuario: root
Puerto:  22
```

Confirmamos que estábamos dentro del servidor Linux porque el
prompt cambió a:

```
root@racknerd-1a89b7a:~#
```

## 3. Copia de los systemd services/timers al VPS

Primero vimos que el servicio no existía:

```
No files found for trashpanda-pilot-bounce-poller.service
```

Copiamos desde la PC al VPS los archivos:

```
trashpanda-retry-worker.service
trashpanda-retry-worker.timer
trashpanda-pilot-bounce-poller.service
trashpanda-pilot-bounce-poller.timer
```

Los pusimos en:

```
/etc/systemd/system/
```

Después:

```bash
systemctl daemon-reload
```

## 4. Password IMAP via systemd override

Editamos:

```bash
systemctl edit trashpanda-pilot-bounce-poller.service
```

Y agregamos el override:

```ini
[Service]
Environment=TRASHPANDA_BOUNCE_IMAP_PASSWORD=****
```

Confirmamos que quedó guardado en:

```
/etc/systemd/system/trashpanda-pilot-bounce-poller.service.d/override.conf
```

El override quedó visible en `systemctl cat`, con la contraseña
correctamente censurada en la captura.

## 5. Activación de los timers

```bash
systemctl enable --now trashpanda-retry-worker.timer
systemctl enable --now trashpanda-pilot-bounce-poller.timer
```

Verificación:

```bash
systemctl list-timers | grep trashpanda
```

Los dos timers quedaron programados para correr automáticamente.

## 6. Problema de versión en el VPS

Al probar los services, fallaron con:

```
No module named app.smtp_retry_worker
No module named app.pilot_send
```

Eso significaba que los `.service` estaban bien, pero el código del
VPS estaba viejo y no tenía los módulos V2.10.11/V2.10.12.

Diagnóstico del repo del VPS — faltaban:

```
app/smtp_retry_worker.py
app/pilot_send/bounce_poller.py
```

El VPS tenía cambios locales en:

```
app/operator_routes.py
app/server.py
configs/production_smtp.yaml
```

y estaba en `main` con el commit `e37f0cf`.

## 7. Actualización de TrashPanda en el VPS

Confirmamos que en la PC local sí existían los módulos nuevos:

```
app\smtp_retry_worker.py
app\pilot_send\bounce_poller.py
```

Backup del TrashPanda anterior:

```
/root/trashpanda_backup_20260506_133012.tar.gz
```

Subida desde Windows. La primera subida por `.tar.gz` falló con:

```
gzip: stdin: unexpected end of file
tar: Unexpected EOF in archive
```

Ese paquete quedó corrupto, pero después el código sí quedó
actualizado y los imports pasaron correctamente:

```
smtp_retry_worker import OK
bounce_poller import OK
```

## 8. Prueba real de los services

Después de actualizar el código:

```bash
systemctl start trashpanda-retry-worker.service
systemctl start trashpanda-pilot-bounce-poller.service
```

Ambos terminaron con éxito:

```
trashpanda-retry-worker.service        → status=0/SUCCESS
trashpanda-pilot-bounce-poller.service → status=0/SUCCESS
```

Reactivamos ambos timers.

Estado final de systemd:

- ✅ retry worker service funciona
- ✅ bounce poller service funciona
- ✅ retry worker timer activo
- ✅ bounce poller timer activo

## 9. Verificación de port 25 outbound

Desde el VPS:

```bash
nc -vz smtp.gmail.com 25
```

Resultado:

```
Connection to smtp.gmail.com 25 port [tcp/smtp] succeeded!
```

✅ **Port 25 outbound abierto.** No necesitamos pagar SMTP relay
ahora mismo — direct-to-MX funciona desde este VPS.

## 10. Verificación de SPF

```bash
dig TXT fuenmayorindustries.com
```

Devolvió:

```
v=spf1 include:_spf.google.com ip4:192.3.105.145 ~all
```

- ✅ SPF existe
- ✅ Google Workspace está autorizado
- ✅ El VPS `192.3.105.145` está autorizado

## 11. Configuración de DKIM

Entramos a Google Admin Console:

```
Apps → Google Workspace → Gmail → Autenticar correo electrónico
```

Google nos dio el record DKIM:

```
Name:  google._domainkey
Type:  TXT
Value: v=DKIM1; k=rsa; p=...
```

Lo agregamos en Cloudflare.

Verificación desde el VPS:

```bash
dig TXT google._domainkey.fuenmayorindustries.com
```

Devolvió `NOERROR` con el TXT DKIM publicado.

✅ **DKIM publicado correctamente.**

## 12. Configuración de DMARC

Primero DMARC no existía:

```
_dmarc.fuenmayorindustries.com → NXDOMAIN
```

Creamos en Cloudflare:

```
Type:    TXT
Name:    _dmarc
Content: v=DMARC1; p=none; rua=mailto:team@fuenmayorindustries.com; adkim=s; aspf=s
TTL:     Auto
```

Verificación:

```bash
dig TXT _dmarc.fuenmayorindustries.com
```

Devolvió:

```
v=DMARC1; p=none; rua=mailto:team@fuenmayorindustries.com; adkim=s; aspf=s
```

✅ **DMARC publicado correctamente.**

---

## Estado final

| Componente | Estado |
|-----------|--------|
| VPS conectado | ✅ |
| TrashPanda actualizado en `/root/trashpanda` | ✅ |
| Backup creado | ✅ |
| systemd services instalados | ✅ |
| retry worker funcionando | ✅ |
| bounce poller funcionando | ✅ |
| timers activos | ✅ |
| Google Workspace App Password configurado | ✅ |
| Port 25 outbound abierto | ✅ |
| SPF configurado | ✅ |
| DKIM configurado | ✅ |
| DMARC configurado | ✅ |

---

## ⚠️ Lo que NO se debe hacer todavía

**No mandar los 100k de golpe.**

El próximo paso correcto es un **pilot controlado**:

- 50–100 emails
- solo candidatos `safe` / `review_low_risk`
- medir hard bounces reales
- dejar que el bounce poller lea los rebotes
- actualizar historial
- decidir si ampliar o bloquear segmento

---

## Referencias

- DNS records de `fuenmayorindustries.com` viven en Cloudflare.
- App Password de Google Workspace: regenerable desde
  `myaccount.google.com → Seguridad → App passwords`.
- Override systemd con la password IMAP:
  `/etc/systemd/system/trashpanda-pilot-bounce-poller.service.d/override.conf`.
- Backup pre-actualización:
  `/root/trashpanda_backup_20260506_133012.tar.gz`.
