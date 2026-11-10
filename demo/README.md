# rift demo

A registry that stores its images through rift, on your machine, in a few minutes. You need Docker
with Compose.

    git clone https://github.com/olegnazarov23/rift
    cd rift/demo
    docker compose up -d

That starts three containers:

- `s3`: an S3 store with one bucket, `registry`. It's [versitygw](https://github.com/versity/versitygw),
  which keeps each object as a file, so you can see on disk what's stored.
- `rift`: the gateway in front of the bucket.
- `registry`: a container registry (distribution 2.8) with rift as its S3 endpoint
  ([registry.yml](registry.yml)), at `localhost:5005`.

## 1. Push some images

    docker compose run --rm push

This copies redis 7.4.0, 7.4.1 and 7.4.2 and postgres 17.2, 17.3 and 17.4 (linux/amd64, 574 MB)
into the registry, each exactly as published. They come from Amazon's mirror of Docker Hub's
official images: the same images, while Docker Hub allows few anonymous pulls an hour.

## 2. Watch rift compact them

rift compacts blobs nobody has written or read for a while: 7 days by default, 20 seconds here.

    docker compose logs -f rift

    rift: compacting 19 cold blobs (304.9 MB)
    rift: compacted 19 of 19 blobs (304.9 MB) in 48.7s (unpacking 36.1s, …)
    rift: compacting 7 cold blobs (269.1 MB)
    rift: compacted 7 of 7 blobs (269.1 MB) in 44.0s (unpacking 37.9s, …)

These times are from a 12-core server; a laptop takes longer. rift 0.1.6 also warns at start that
it couldn't read the bucket's lifecycle rules: versitygw doesn't have any.

## 3. Compare

    docker compose run --rm stats

    The registry sees     574.1 MB
    The bucket holds      236.6 MB  by rift's count
                          236.6 MB  in files on disk
    Saved                  58.8 %
    Blobs compacted          26

The registry sees every blob at its original size. The bucket holds rift's packs and manifests,
and everything rift leaves as it is: the registry's own small files, image configs and layers
under 1 MB. The first figure for it comes from rift; the second adds up the bucket's files.

## 4. Read everything back

    docker compose run --rm check

This downloads every image through the registry, and so through rift, and checks each layer
against its digest:

    PASS: registry:5000/redis:7.4.0
    PASS: registry:5000/redis:7.4.1
    …

Your own Docker can pull them too:

    docker pull localhost:5005/redis:7.4.2

After rebuilding a layer for a read, rift puts the original back in the bucket, so the next pull is
a plain download. Once nobody has read it for 20 seconds the stub goes back, so `stats` shows less
saved for a minute after a read.

## Your own images

`push` and `check` take any public images: a registry and path in `SOURCE`, names in `IMAGES`. The
more versions of one image, the more they share.

    SOURCE=ghcr.io/you IMAGES="app:1.0 app:1.1 app:1.2" docker compose run --rm push
    SOURCE=ghcr.io/you IMAGES="app:1.0 app:1.1 app:1.2" docker compose run --rm check

## Clean up

    docker compose down -v
