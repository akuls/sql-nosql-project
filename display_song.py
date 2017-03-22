"""
Thierry Bertin-Mahieux (2010) Columbia University
tb2332@columbia.edu

Code to quickly see the content of an HDF5 file.

This is part of the Million Song Dataset project from
LabROSA (Columbia University) and The Echo Nest.


Copyright 2010, Thierry Bertin-Mahieux

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import sys
import hdf5_getters
import numpy as np

PATH = "D:\Learning\Graduate\UMass\Course_content\Sem_2\DB\Project\Dataset\MillionSongSubset\data"

def die_with_usage():
    """ HELP MENU """
    print 'display_song.py'
    print 'T. Bertin-Mahieux (2010) tb2332@columbia.edu'
    print 'to quickly display all we know about a song'
    print 'usage:'
    print '   python display_song.py [FLAGS] <HDF5 file> <OPT: song idx> <OPT: getter>'
    print 'example:'
    print '   python display_song.py mysong.h5 0 danceability'
    print 'INPUTS'
    print '   <HDF5 file>  - any song / aggregate /summary file'
    print '   <song idx>   - if file contains many songs, specify one'
    print '                  starting at 0 (OPTIONAL)'
    print '   <getter>     - if you want only one field, you can specify it'
    print '                  e.g. "get_artist_name" or "artist_name" (OPTIONAL)'
    print 'FLAGS'
    print '   -summary     - if you use a file that does not have all fields,'
    print '                  use this flag. If not, you might get an error!'
    print '                  Specifically desgin to display summary files'
    sys.exit(0)


def get_song_info(song_path):

    # get params
    hdf5path = song_path
    songidx = 0
    onegetter = ''

    # if len(sys.argv) > 2:
    #     songidx = int(sys.argv[2])
    # if len(sys.argv) > 3:
    #     onegetter = sys.argv[3]

    # sanity check
    if not os.path.isfile(hdf5path):
        print 'ERROR: file',hdf5path,'does not exist.'
        sys.exit(0)
    h5 = hdf5_getters.open_h5_file_read(hdf5path)
    numSongs = hdf5_getters.get_num_songs(h5)
    if songidx >= numSongs:
        print 'ERROR: file contains only',numSongs
        h5.close()
        sys.exit(0)

    # get all getters
    getters = filter(lambda x: x[:4] == 'get_', hdf5_getters.__dict__.keys())
    
    print getters

    getters.remove("get_num_songs") # special case
    #Remove the fields that we don't need
    getters = np.sort(getters)

    # print them
    for getter in getters:
        try:
            res = hdf5_getters.__getattribute__(getter)(h5,songidx)
        except AttributeError, e:
            if summary:
                continue
            else:
                print e
                print 'forgot -summary flag? specified wrong getter?'
        if res.__class__.__name__ == 'ndarray':
            print getter[4:]+": shape =",res.shape
        else:
            print getter[4:]+":",res

    # done
    print 'DONE, showed song',songidx,'/',numSongs-1,'in file:',hdf5path
    h5.close()


if __name__ == '__main__':
    # for dirname, dirnames, filenames in os.walk(PATH):
    #     # print path to all subdirectories first.
    #     # for subdirname in dirnames:
    #     #     print(os.path.join(dirname, subdirname))

    #     # print path to all filenames.
    #     for filename in filenames:
    #         print(os.path.join(dirname, filename))

    with open("all_files.txt", 'r') as f:
            content = f.readlines()

    content = [x.strip() for x in content]

    #Iterate over each file path and get the song details
    for each_file in content:
        get_song_info(each_file)
        break