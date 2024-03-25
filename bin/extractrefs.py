import json
import os
import sys
import refextract


if __name__ == "__main__":
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    references = refextract.extract_references_from_file(input_filename)
    arxiv_id = os.path.basename(input_filename).replace('.pdf', '')
    json.dump(
        {
            "arxiv_id": arxiv_id,
            "references": references,
        },
        open(output_filename, 'wt')
    )
