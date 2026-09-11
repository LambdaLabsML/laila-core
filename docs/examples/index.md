# Examples

End-to-end walkthroughs that combine several LAILA features into one workflow. Where the [Tutorials](../tutorials/01_entries_and_identity.md) each introduce a single concept, an example builds something you would run for real. Every page has a matching Jupyter notebook you can download from the button under its title.

## Image dataset pipeline

Two examples that together form a complete dataset workflow on Cloudflare R2:

1. [Dataset Creation — Random Images to Cloudflare R2](01_dataset_creation.md)
   Set up an R2 bucket and API token, generate random PNG and JPEG images, store each one as an entry whose payload is the raw file bytes, and index them all under a `Manifest` named `my_dataset`.

2. [Data Loader — Prefetching Through memory << hdd << cloudflare](02_data_loader.md)
   Wire `laila.alpha_pool << hdd << r2`, then write a `LailaDataLoader` that keeps four batches in flight, runs `laila.remember` and a bytes-to-`torch.Tensor` cast per sample on a LAILA taskforce, and `forget`s each batch from memory (and optionally disk) once the model has consumed it.

**Requires:** `pip install "laila-core[cloudflare,hdf5,torch]" pillow` and a `secrets.toml` with Cloudflare R2 credentials. Example 1 walks through obtaining them.
