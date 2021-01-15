from cumulus_process import Process, s3
import os
from re import match, search
import copy
from cumulus_process.s3 import download, upload


class DMRPPGenerator(Process):
    """
    Class to generate dmrpp files from hdf and netCDf files
    The input will be *.nc *nc4 *.hdf
    The output *.nc.dmrpp *nc4.dmrpp *.hdf.dmrpp
    """

    def __init__(self, **kwargs):
        self.processing_regex = '.*\\.(h(e)?5|nc(4)?)(\\.bz2|\\.gz|\\.Z)?'
        super(DMRPPGenerator, self).__init__(**kwargs)
        self.path = self.path.rstrip('/') + "/"


    @property
    def input_keys(self):

        return {
            'input_files': f"{self.processing_regex}(\\.cmr\\.xml|\\.json)?$"
        }


    def get_bucket(self, filename, files, buckets):
        """
        Extract the bucket from the files
        :param filename: Granule file name
        :param files: list of collection files
        :param buckets: Object holding buckets info
        :return: Bucket object
        """
        bucket_type = "public"
        for file in files:
            if match(file.get('regex', '*.'), filename):
                bucket_type = file['bucket']
                break
        return buckets[bucket_type]

    
    def upload_file(self, filename):
        """ Upload a local file to s3 if collection payload provided """
        info = self.get_publish_info(filename)
        if info is None:
            return filename
        try:
            return s3.upload(filename, info['s3'], extra={}) if info.get('s3', False) else None
        except Exception as e:
            self.logger.error("Error uploading file %s: %s" % (os.path.basename(os.path.basename(filename)), str(e)))

    
    def upload_output_files(self):
        """ Uploads all self.outputs """
        
        uploaded_files = {}
        for granule_id, file in self.output.items():
            uploaded_files[granule_id] = self.upload_file(file)
        return uploaded_files


    def update_input(self):
        """Function to update self.input into a dictionary of granules and netcdf or hdf file"""

        regex = self.input_keys.get('input_files', None)
        if regex is None:
            raise Exception('No files matching %s' % regex)

        input_files = {}
        self.original_input = copy.deepcopy(self.input)
        for granule in self.input.get('granules'):
            granule_id = granule.get('granuleId')
            for file in granule.get('files'):
                file_name = file.get('filename')
                m = match(regex, file_name)
                if m is not None:
                    input_files[granule_id] = file_name

        self.input = input_files


    def fetch(self, key, remote=False):
        """ Get local (default) or remote input filename """

        outfiles = {}
        for granule_id, file in self.input.items():
            if remote or os.path.exists(file):
                outfiles[granule_id] = file
            else:
                fname = download(file, path=self.path)
                outfiles[granule_id] = fname
                self.downloads.append(fname)
        return outfiles


    def process(self):
        """
        Override the processing wrapper
        :return:
        """

        self.update_input()
        input_files = self.fetch('input_files')
        self.output = self.dmrpp_generate(input_files)
        uploaded_files = self.upload_output_files()
        collection = self.config.get('collection')
        buckets = self.config.get('buckets')

        files_sizes = {}
        for granule_id, output_file_path in self.output.items():
            files_sizes[os.path.basename(output_file_path)] = os.path.getsize(output_file_path)
            os.remove(output_file_path)

        granule_data = {}
        for granule_id, uploaded_file in uploaded_files.items():
            if uploaded_file is None or not uploaded_file.startswith('s3'):
                continue
            filename = uploaded_file.split('/')[-1]
            new_file = {
                "bucket": self.get_bucket(filename, collection.get('files', []),buckets)['name'],
                "filename": uploaded_file,
                "name": filename,
                "size": files_sizes.get(filename, 0),
                "filepath": "{}/{}".format(self.config.get('fileStagingDir',""), filename)
            }
            granule_data[granule_id] = new_file

        for idx, val in enumerate(self.original_input.get('granules')):
            granule_id = val.get('granuleId')
            dmrpp_data = granule_data.get(granule_id)
            if dmrpp_data:
                self.original_input['granules'][idx]['files'].append(dmrpp_data)
            
        return self.original_input


    def get_data_access(self, key, bucket_destination):
        """
        param key: filename
        param bucket_destination: destination bucket will the file exist
        return: access URL
        """
        key = key.split('/')[-1]
        half_url = ("%s/%s/%s" % (bucket_destination, self.config['fileStagingDir'], key)).replace('//','/')
        return "%s/%s"% (self.config.get('distribution_endpoint').rstrip('/'), half_url)


    def dmrpp_generate(self, input_files):
        """
        """
        outputs = {}
        for granule_id, input_file in input_files.items():
            if not match(f"{self.processing_regex}$", input_file):
                outputs += [input_file]
                continue
            cmd = f"get_dmrpp -b {self.path} -o {input_file}.dmrpp {os.path.basename(input_file)}"
            self.run_command(cmd)
            outputs[granule_id] = f"{input_file}.dmrpp"
        return outputs


if __name__ == "__main__":
    DMRPPGenerator.cli()