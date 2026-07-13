Plex + Prowlarr/Sonarr/Radarr + Transmission, with Transmission's traffic
routed through Mullvad via Gluetun. Sonarr/Radarr/Prowlarr/Plex stay on the
normal network for fast LAN access and indexer speed - only the download
client is VPN-tunneled.

## Folder layout on /mnt/media

Sonarr, Radarr, Transmission, and Plex all mount the entire `/mnt/media` NFS
export at the same container path. Keep downloads and library folders as
subdirectories of that one export (e.g. `/mnt/media/downloads`,
`/mnt/media/movies`, `/mnt/media/tv`) - Sonarr/Radarr rely on hardlinking
between the downloads folder and the library folders to avoid duplicating
files on import, which only works within a single filesystem/export.

## First-time setup

1. Generate a Mullvad WireGuard key at mullvad.net/en/account and fill in
   `MULLVAD_WIREGUARD_PRIVATE_KEY` / `MULLVAD_WIREGUARD_ADDRESSES`
2. Grab a fresh claim token from https://plex.tv/claim (expires in 4 minutes)
   right before deploying, for `PLEX_CLAIM`
3. In Prowlarr, add indexers, then connect Sonarr/Radarr as "Apps" so
   Prowlarr syncs indexers to both automatically
4. In Sonarr/Radarr, add Transmission as the download client, pointing at
   `gluetun:9091` (Transmission's ports live on the gluetun container due to
   `network_mode: service:gluetun`)

## Known limitation

Transmission's incoming peer port is not forwarded through the home
network's router - since its traffic exits via Mullvad, port forwarding
would need to be configured on the Mullvad side instead. Without it,
downloads still work but connect to fewer peers/slower swarms. Not set up
here; revisit if download speeds are a problem.
