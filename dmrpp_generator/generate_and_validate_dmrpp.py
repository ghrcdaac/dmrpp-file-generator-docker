#! /usr/bin/python3
import argparse
import subprocess
import time
import os
from multiprocessing import Process
import tempfile


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, separate_bar='-', length=100, fill='█',
                     printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        separate_bar - Optional : what will separate the bar as it fills
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + separate_bar * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def generate_docker_compose():
    _, dockercompose_file_location = tempfile.mkstemp(suffix=".yml")
    with open(dockercompose_file_location, 'w') as dc:
        dc.write(
            """
version: '3'
services:
  dmrpp:
    # Path to dockerfile.
    # '.' represents the current directory in which
    # docker-compose.yml is present.
    image: ghrcdaac/dmrpp-generator:v3.4.0
    environment:
      - PAYLOAD=${PAYLOAD}
    # Mount volume
    volumes:
      - ${NC_FILES_PATH:-/tmp}:/usr/share/hyrax

  hyrax:

    # image to fetch from docker hub
    image: opendap/hyrax:snapshot
    ports:
    - "${PORT:-8080}:8080"
    volumes:
      - ${NC_FILES_PATH:-/tmp}:/usr/share/hyrax/
    working_dir: /usr/share/hyrax
    container_name: hyrax

            """
        )
    return dockercompose_file_location


def progress_bar(file_number, prefix='Generating:', suffix='Complete', length=50, fill='█', separate_bar='-'):
    items = list(range(0, min(file_number * 25, 600)))
    l = len(items)
    # Initial call to print 0% progress
    printProgressBar(iteration=0, total=l, prefix=prefix, suffix=suffix, length=length, fill=fill,
                     separate_bar=separate_bar)
    for i, _ in enumerate(items):
        time.sleep(0.1)
        # Update Progress Bar
        printProgressBar(iteration=i + 1, total=l, prefix=prefix, suffix=suffix, length=length, fill=fill,
                         separate_bar=separate_bar)


def check_docker_version(log_file_path):
    with open(log_file_path, "a+") as output:
        dkr_comp_version = 'docker compose'
        subprocess.call(f"{dkr_comp_version} version", shell=True, stdout=output, stderr=output)
        output.seek(0)
        err_grab = output.readlines()[-1]
        if err_grab == f'/bin/sh: 1: {dkr_comp_version}: not found\n':
            dkr_comp_version = 'docker-compose'
    return dkr_comp_version


def run_docker_compose(payload, nc_hdf_path, port, dmrrpp_service, log_file_path):
    dockercompose_file_location = generate_docker_compose()
    dkr_comp_version = check_docker_version(log_file_path)
    with open(log_file_path, "a") as output:
        try:
            subprocess.call(
                f"PAYLOAD='{payload}' NC_FILES_PATH={nc_hdf_path} PORT={port} {dkr_comp_version} -f {dockercompose_file_location} up {dmrrpp_service}",
                shell=True, stdout=output,
                stderr=output)
        except KeyboardInterrupt:
            subprocess.call(f" {dkr_comp_version} -f {dockercompose_file_location} down {dmrrpp_service}", shell=True,
                            stdout=output,
                            stderr=output)
            os.remove(dockercompose_file_location)


def main():
    parser = argparse.ArgumentParser(description='Generate and validate DMRPP files.')
    parser.add_argument('-p', '--path', dest='nc_hdf_path', nargs=1, required=True,
                        help='Path to netCDF4 and HDF5 folder')
    parser.add_argument('-prt', '--port', dest='port', nargs=1, default=["8080"],
                        help='Port number to Hyrax local server')
    parser.add_argument('-pyld', '--payload', dest='payload', nargs=1, default=['{}'],
                        help='Payload to execute get_dmrpp binary')
    parser.add_argument('-vldt', '--validate', dest='validate', nargs=1, default=['true'],
                        help='Validate netCDF4 and HDF5 files against OPeNDAP local server')

    args = parser.parse_args()
    nc_hdf_path, port, payload, validate = [getattr(args, ele)[0] for ele in args.__dict__.keys()]
    no_need_validation = validate not in ['true', '1', 'yes', 'y']
    # If the user doesn't want validation run dmrpp service alone without Hyrax UI
    dmrrpp_service = "dmrpp" if no_need_validation else ""
    # Depending on needing the validation the user should get either a path or Hyrax UI link
    visit_link_path_message = f"{nc_hdf_path}" if no_need_validation else f"http://localhost:{port}/opendap (^C to kill the server)"

    # Counting number of files to estimate the work
    _, _, files = next(os.walk(nc_hdf_path))
    # Remove dmrpp suffix from the list of files
    [files.remove(file_) for file_ in files[:] if file_.endswith('.dmrpp')]
    _, log_file_location = tempfile.mkstemp(prefix="dmrpp-generator-")
    message_visit_server = "" if no_need_validation else f"To see the results visit ( 🌎 ):\t{visit_link_path_message}"
    try:
        p1 = Process(target=progress_bar, args=(len(files),))
        p2 = Process(target=run_docker_compose, args=(payload, nc_hdf_path, port, dmrrpp_service, log_file_location))
        p1.start()
        p2.start()
        p1.join()
        print(f"{message_visit_server}\nLogs are located here (🪵 ):\t{log_file_location}")
        p2.join()
    except KeyboardInterrupt:
        print("Shutting down the server...")
        p1 = Process(target=progress_bar, args=(3, "Progress", 'Complete', 10, '💀', '🔥',))
        p1.start()
        p1.join()


if __name__ == "__main__":
    main()
