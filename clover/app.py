import constants 
from parsers import spec, data, base
if __name__ == "__main__":
    spec_parser = spec.Spec()
    spec_parser.load_specs()
    spec_parser.close_conn()

    data_parser = data.Data()
    data_parser.consume_data_files()
    data_parser.close_conn()

