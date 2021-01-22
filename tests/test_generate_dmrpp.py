import os
from unittest import TestCase
import json
from dmrpp_generator.main import DMRPPGenerator
from unittest.mock import patch

class StorageValues:
    processing_output = None


class TestDMRPPFileGeneration(TestCase):
    """
    Test generating dmrpp files.
    """
    granule_id = "tpw_v07r01_201910"
    granule_name = "tpw_v07r01_201910.nc"
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures")
    input_file = {
        "granules": [
            {
                "granuleId": granule_id,
                "dataType": "MODIS_A-JPL-L2P-v2019.0",
                "sync_granule_duration": 3759,
                "files": [
                  {
                    "bucket": "fake-cumulus-protected",
                    "path": "fakepath/2020/001",
                    "filename": f"{fixture_path}/{granule_name}",
                    "size": 18232098,
                    "name": granule_name,
                    "checksumType": "md5",
                    "checksum": "aa5204f125ae83847b3b80fa2e571b00",
                    "type": "data",
                    "url_path": "",
                    "filepath": granule_name,
                    "duplicate_found": True
                  },
                  {
                    "bucket": "fake-cumulus-public",
                    "path": "fakepath/2020/001",
                    "filename": f"s3://fake-cumulus-public/{granule_name}.md5",
                    "size": 98,
                    "name": f"{granule_name}.md5",
                    "type": "metadata",
                    "url_path": "",
                    "filepath": f"{granule_name}.md5",
                    "duplicate_found": True
                  },
                  {
                    "bucket": "fake-cumulus-public",
                    "filename": f"s3://fake-cumulus-public/{granule_name}.cmr.json",
                    "fileSize": 1381,
                    "name": f"{granule_name}.cmr.json",
                    "type": "metadata",
                    "url_path": "",
                    "filepath": f"{granule_name}.cmr.json"
                  }
                ],
                "version": "2019.0"
            }
        ]
    }

    payload_file = f"{fixture_path}/payload.json"
    with open(payload_file) as f:
        payload = json.load(f)

    process_instance = DMRPPGenerator(input = input_file, config=payload['config'], path=fixture_path)

    @patch('dmrpp_generator.main.DMRPPGenerator.upload_output_files',
       return_value={granule_id:f's3://{granule_name}.dmrpp'})
    @patch('cumulus_process.Process.fetch_all',
       return_value={'input_key': [os.path.join(os.path.dirname(__file__), f"fixtures/{granule_name}")]})
    @patch('os.remove', return_value=granule_name)
    def test_1_check_generate_dmrpp(self, mock_upload, mock_fetch, mock_remove):
        """
        Testing get correct start date
        :return:
        """
        StorageValues.processing_output = self.process_instance.process()
        expected_file_path = f"{self.process_instance.path}/{self.granule_name}.dmrpp"
        self.assertEqual(os.path.exists(expected_file_path), 1)

    def test_2_check_output(self):
        """
        Test the putput schema of the processing
        :return:
        """
        print(StorageValues.processing_output)
        self.assertListEqual(['granules'], list(StorageValues.processing_output.keys()))

    def test_3_checkout_dmrpp_output(self):

        dmrpp_file = f"{self.granule_name}.dmrpp"
        dmrpp_exists = False
        for granules in StorageValues.processing_output.get('granules'):
            for file in granules.get('files'):
                if file["name"] == dmrpp_file:
                    dmrpp_exists = True
        self.assertEqual(True, dmrpp_exists)

