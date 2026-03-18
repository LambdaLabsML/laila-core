in utils, add a class for reading arguments from the following filetypes:

.env
.json
.toml
.xml

as well as reading from terminal directly. 

consider the above nested at 1 level, so basically key=>value



each arg given by user goes into laila.args.something, where something is the argument. It is defined as a dotmap in the __init__ at top level. 

add test cases for each of them in the tests folder as unit tests with their corresponding ipynb files. Test cases should read args like s3_bucket_region, but the actual names is up to the user and passed at runtime. 





