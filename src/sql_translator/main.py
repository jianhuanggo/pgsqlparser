import sys
from pathlib import Path
from sqlglot.errors import ParseError
from translator import SQLTranslator


def main():
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_sql_file> <output_sql_file>")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)

    # Read input SQL
    with open(input_file, 'r') as f:
        sql = f.read()

    try:
        # Translate SQL
        translator = SQLTranslator()
        translated_sql = translator.translate(sql)

        # Write output SQL
        with open(output_file, 'w') as f:
            f.write(translated_sql)
    except ParseError as e:
        print(f"Error parsing SQL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error translating SQL: {e}")
        sys.exit(1)

    print(f"Successfully translated SQL from {input_file} to {output_file}")


if __name__ == "__main__":
    main()
