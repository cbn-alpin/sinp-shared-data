# sinp-shared-data
Contient les scripts et fichiers partagés entre les dépôts sinp-paca-data et sinp-aura-data.

## Synchronisation serveur

Pour transférer uniquement le dossier `shared/` sur le serveur, utiliser `rsync`
en testant avec l'option `--dry-run` (à supprimer quand tout est ok):

```bash
rsync -av --copy-unsafe-links --exclude .git --exclude .gitignore --exclude data/raw/ --exclude config/settings.ini ./ geonat@db-<region>-sinp:~/data/shared/ --dry-run
```
