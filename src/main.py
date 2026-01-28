from orchestrators import list_metadata, fetch_datasets

def main():
    list_metadata.run()
    fetch_datasets.run()

if __name__ == "__main__":
    main()


