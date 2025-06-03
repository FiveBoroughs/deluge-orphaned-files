---
trigger: always_on
---

Here is the logic I'm looking for with deletions:
 - Files that are only in torrents on disk but not in the deluge client are failures from the auto removal plugin in deluge that happens a couple times a week, they should be actually deleted after the time between first seen and last seen is more than 7 days, and more that 7 consecutinve scans just to be sure.
 - Files that are only in torrents on disk and in the othercat label/category are files that will be deleted by the auto removal deluge plugin according to it's own logic. If a file is only on the torrent side and not in othercat, something is probably wrong and needs to be looked into but most likely the file has just been replaced by a better release like 1080p trumping 720p, and so ther 720p file likely just needs to be applied the othercat label/category in deluge.
 - Files that are only in media on disk should rarely happen, probably means they were incorrect given the othercat label in deluge and deleted on torrents disk side by the auto removal deluge plugin, they should be manually looked into

And Here is the use for the script:
 - The user has a big library of files in media for plex viewing, they are managed by radarr/sonarr and better files are downloaded with deluge when available. Radarr and sonarr instruct deluge to download the files to the torrents disk side, and then hard link to the media disk side (imported) to be viewed. This process leaves a lot of files behind called orphans, files discarted for better ones. This script is made to help with the clean up.
 - The script is aimed to be ran in a docker container in a service like mode and at a set time self triggered to run a scan
 - The output of the scan is mailed the user, with problematic files, their path, size and source.
 - The user is informed of the action that will be taken in x days if he doesn't stop it.

Example email :

Path | Label | Size | Source | Resolution | When
tv/MobLand.S01E09.Beggars.Banquet.2160p.AMZN.WEB-DL.DDP5.1.H.265-RAWR.mkv | None | 5.13 GB | local_torrent_folder | Deletion | 7 days
tv/MobLand.S01E09.Beggars.Banquet.2160p.PMTP.WEB-DL.DDP5.1.DV.HDR.H.265-RAWR.mkv | tvcat.cross-seed | 4.85 GB | torrents | Label as othercat | 7 days
tv/MobLand.S01E09.Beggars.Banquet.1080p.AMZN.WEB-DL.DDP5.1.H.264-RAWR.mkv | None | 3.31 GB | media | Manual review | N/A