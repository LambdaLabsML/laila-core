create a new pool type, which takes in a file destination for an iso or img file. Store the image in the ~/.laila/pools/filesystem/pool's global_id.img, or a user specified destination. 

Then the pool is mounted /mnt/pool's global id. 

The pool's default transformation is base_64. It then stores the inputs as .json. 

Make sure to close the handle to files upon writing. 

When reading a via pool's getitem make sure to open for append so you can modify the file if needed. 