#! /usr/bin/env python3

import sys
import ftplib
import os
import re
import datetime

def getDirDate(inDir=''):
    """Strip the date from the cdas2 directory name.

    The directory name that contains the cdas2 files has the pattern
    cdas2.YYYYmmdd.  This function will verify the directory contains
    the correct format, and then return the date portion as a datetime
    object.

    If the directory does not follow the correct format, then return
    None.
    """

    # Default return value
    oDate = None

    # Verify the directory name has the correct format:
    if re.fullmatch(r'cdas2\.\d{8}', inDir):
        oDate = datetime.datetime.strptime(inDir, 'cdas2.%Y%m%d')
    return oDate

def getFile(ftp='', source='', target=''):
    """Download the ftp file listed in source, and place in target

    ftp must be a ftplib.FTP instace, and logged in.

    This function will check if the file exists.  If it does exist,
    then it will check the size and date stamp.  If the file on the
    ftp site is newer, or a different size, then the file will be
    downloaded again.  If the size is the same, and the date stamp on
    the ftp site is older, then the download will not be retried.

    This funtion will return True if successful (or if the file didn't
    need to be downloaded).
    """

    # Default return value
    myRet=False

    # Check if ftp is an ftplib.FTP instance
    if isinstance(ftp, ftplib.FTP):
        # Check if the target file exists

        # To indicate if the download should be attempted.
        doDownload=True

        # Check if the target file exists
        if os.path.isfile(target):
            # Need to get file sizes and ctime
            try:
                target_size=os.path.getsize(target)
                target_ctime=datetime.datetime.fromtimestamp(os.path.getctime(target))
            except OSError as err:
                print("WARNING: Unable to get the size or ctime of the target file \"{0}\".".format(target), file=sys.stderr)
                print("WARNING: Retrying the download. ([{0}] {1})".format(err.errno, err.strerror), file=sys.stderr)
            else:
                # Need to get source size and ctime
                try:
                    source_size=ftp.size(source)
                    source_ctime_str=ftp.sendcmd('MDTM {}'.format(source))
                    source_ctime=datetime.datetime.strptime(source_ctime_str, '213 %Y%m%d%H%M%S')
                except ftplib.all_errors as err:
                    print("WARNING: Unable to get the size or ctime of the source file \"{0}\".".format(source), file=sys.stderr)
                    print("WARNING: Retrying the download. ({1})".format(err), file=sys.stderr)

                # Check if the files are the _same_. Same here is that
                # the file sizes are the same, and the source ctime is
                # older than the target's ctime.
                if source_size == target_size and source_ctime < target_ctime:
                    print("NOTE: File \"{0}\" already retrieved.".format(source), file=sys.stderr)
                    doDownload = False
                else:
                    print("WARNING: Target \"{0}\" exists, but does not match the source \"{1}\".".format(target, source), file=sys.stderr)
                    print("WARNING: Retrieving.", file=sys.stderr)
        # Now do the download
        try:
            ftp.retrbinary('RETR {}'.format(source), open(target, 'wb').write)
        except ftplib.all_errors as err:
            print("WARNING: Error while attemptint to retrieve file \"{0}\". ({1})".format(source, err), file=sys.stderr)
        except OSError as err:
            print("WARNING: Unable to write target file \"{0}\". ([{1}] {2})".format(target, err.errno, err.strerror), file=sys.stderr)
        else:
            myRet = True
    return myRet
            
def main():
    """Download cdas2 files from ftp.ncep.noaa.gov

    This application will download the files from ncep for use in the
    GFDL seasonal prediction.

    TODO: Add real logging (to a file)
    TODO: Add a configuration file
    """
    
    ftpUrl="ftp.ncep.noaa.gov"
    ftpPath="pub/data/nccf/com/cdas2/prod"
    
    OUTPUT_DIR="/home/sdu/Development/nmme/pythonFtpTests/ncep_reanal/testData"

    # Make sure OUTPUT_DIR exists.
    # Exit if it doesn't.
    if not os.path.isdir(OUTPUT_DIR):
        exit("ERROR: Directory \"{0}\" does not exist.  Please create, and try again.".format(OUTPUT_DIR))

    # Connect to host, default port
    try:
        ftp=ftplib.FTP(ftpUrl)
    except ftplib.all_errors as err:
        exit("ERROR: Unable to connect to ftp site \"{0}\": ({1}).".format(ftpUrl, re.sub(r'\[.+\]', '', str(err)).strip()))

    
    # Anonymous login
    try:
        ftp.login()
    except ftplib.all_errors as err:
        # Clean up ftp connection, and exit
        ftp.quit()
        exit("ERROR: Unable to login to ftp site \"{0}\": ({1}).".format(ftpUrl, err))

    # change to the correct directory
    try:
        ftp.cwd(ftpPath)
    except ftplib.all_errors as err:
        # Clean up ftp connection, and exit
        ftp.quit()
        exit("ERROR: Unable to change to directory \"{0}\": ({1}).".format(ftpUrl, err))

    # Get the names of all directories in the cwd
    try:
        dirs=ftp.nlst()
    except ftplib.all_errors as err:
        exit("ERROR: Unable to list directories: ({0}).".format(err))

    for inDir in dirs:
        # Get the date from the filename, which is the extension of the
        # directory, and remove the '.' from the extension.
        dirDate=getDirDate(inDir)
        if not dirDate:
            print("WARNING: Not able to extract the date from the directory name.  Skipping {1} . . .".format(inDir),
                  file=sys.stderr)
            continue
        
        # Set the output directory to be YYYYmmm where mmm is the
        # lowercase month abbreviation.
        outDir=dirDate.strftime('%Y%b').lower()

        fullOutDir=os.path.join(OUTPUT_DIR, outDir)
        # Need to make sure the output directory exists
        if not os.path.isdir(fullOutDir):
            try:
                os.mkdir(fullOutDir)
            except OSError as err:
                print("WARNING: Unable to create directory \"{0}\".  Skipping all files in \"{1}\". ([{3}] {4})".format(fullOutDir,
                                                                                                                        inDir,
                                                                                                                        err.errno,
                                                                                                                        err.strerror),
                      file=sys.stderr)
                continue
                         
        print("NOTE: Files from directory \"{0}\" will be placed in \"{1}\".".format(inDir, outDir), file=sys.stderr)
        # Enter the directory
        try:
            ftp.cwd(inDir)
        except ftplib.all_errors as err:
            print("WARNING: Unable to enter ftp directory \"{0}\".  Skipping . . .".format(inDir),
                  file=sys.stderr)

        # Get a list of file in the new directory
        try:
            files=ftp.nlst()
        except ftplib.all_errors as err:
            print("WARNING: Unable to get a list of file in directory \"{0}\".  Skipping . . .".format(inDir), file=sys.stderr)
            ftp.cwd("..")
            continue
        
        for inFile in files:
            # The inFile names have the format: cdas2.t??z.sanl
            # where ?? is the two digit hour.
        
            # Set the output file name, need the date from the directory,
            # will have the format: sig.anl.YYYYMMDDHH.
            outFile="sig.anl.{0}{1}.ieee".format(dirDate.strftime('%Y%m%d'), inFile[7:9])

            # Download the file
            getFile(ftp, inFile, os.path.join(fullOutDir, outFile))
        # Return to the parent directory
        ftp.cwd("..")
    ftp.quit()



if __name__ == '__main__':
    main()
