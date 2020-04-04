# reflector-s3-cleaner
[![Build Status](https://travis-ci.com/nikooo777/reflector-s3-cleaner.svg?branch=master)](https://travis-ci.com/nikooo777/reflector-s3-cleaner)

this tool checks all the streams that were reflected, compares them to the blockchain data indexed in chainquery and eventually deletes all those streams that aren't in the blockchain

# Compiling
```bash
make test #optional
make
```
you will find the binary in ./bin/

# Configuration

```bash
cp config.json.example config.json
```
edit the configuration file and run

# Usage
```
./reflector-s3-cleaner --help
cleanup reflector storage

Usage:
  reflector-s3-cleaner [flags]

Flags:
      --existing_hashes string     path of sd_hashes that exist on chain (default "existing_sd_hashes.json")
  -h, --help                       help for reflector-s3-cleaner
      --limit int                  how many streams to check (approx) (default 50000000)
      --load-chainquery-data       load results from file instead of querying the chainquery database unnecessarily
      --load-reflector-data        load results from file instead of querying the reflector database unnecessarily
      --save-chainquery-data       save results to file once loaded from the chainquery database
      --save-reflector-data        save results to file once loaded from the reflector database
      --sd_hashes string           path of sd_hashes (default "sd_hashes.json")
      --unresolved_hashes string   path of sd_hashes that don't exist on chain (default "unresolved_sd_hashes.json")
```