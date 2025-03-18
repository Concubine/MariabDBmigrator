import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Test argument parsing")
    subparsers = parser.add_subparsers(dest="mode", help="Operation mode")
    
    import_parser = subparsers.add_parser("import", help="Import mode")
    import_parser.add_argument("--input", type=str, required=True)
    
    print("Raw arguments:", sys.argv)
    args = parser.parse_args()
    print("Parsed arguments:", args)

if __name__ == "__main__":
    main() 