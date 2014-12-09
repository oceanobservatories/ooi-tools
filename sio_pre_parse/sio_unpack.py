__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re
import os
import pickle

import mdd_config

# SIO block end sentinel:
SIO_BLOCK_END = b'\x03'

# SIO controller header:
SIO_HEADER_REGEX = b'\x01'                  # Start of SIO Header (start of SIO block)
SIO_HEADER_REGEX += b'(AD|CT|CO|DO|FL|PH|CS|PS|WA|WC|WE)'  # 2 char Instrument IDs
SIO_HEADER_REGEX += b'[0-9]{5}'             # Controller ID
SIO_HEADER_REGEX += b'[0-9]{2}'             # Number of Instrument / Inductive ID
SIO_HEADER_REGEX += b'_'                    # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{4})'     # Number of Data Bytes (hex)
SIO_HEADER_REGEX += b'[0-9A-Za-z]'          # MFLM Processing Flag (coded value)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{8})'     # POSIX Timestamp of Controller (hex)
SIO_HEADER_REGEX += b'_'                    # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{2})'     # Block Number (hex)
SIO_HEADER_REGEX += b'_'                    # Spacer (0x5F)
SIO_HEADER_REGEX += b'([0-9a-fA-F]{4})'     # CRC Checksum (hex)
SIO_HEADER_REGEX += b'\x02'                 # End of SIO Header (binary data follows)
SIO_HEADER_MATCHER = re.compile(SIO_HEADER_REGEX)

SIO_HEADER_LENGTH = 34

# sio header group match index
SIO_HEADER_GROUP_ID = 1           # Instrument ID
SIO_HEADER_GROUP_DATA_LENGTH = 2  # Number of Data Bytes

sio_db_file = mdd_config.datafile('sio.pckl')

# constants for accessing unprocessed data
START_IDX = 0
END_IDX = 1

# map of instrument ID to file type to place instrument data in
ID_MAP = {
    'AD': 'adcps',
    'CT': 'ctdmo',
    'CO': 'ctdmo',
    'DO': 'dosta',
    'FL': 'flort',
    'PH': 'phsen',
    'CS': 'status',
    'PS': 'status',
    'WA': 'wa_wfp',
    'WC': 'wc_wfp',
    'WE': 'we_wfp'}  # dosta_ln_wfp, flord_l_wfp, wfp_eng


class StateKey(object):
    UNPROCESSED_DATA = 'unprocessed_data'
    FILE_SIZE = 'file_size'
    OUTPUT_INDEX = 'output_index'


class SioFileStateInit(object):
    def __init__(self):
        self.file_state = {}


class SioState(object):

    def __init__(self):
        """
        Either load the file state from the pickle, or initialize it
        """
        try:
            self.sio_db = pickle.load(open(sio_db_file))
            print "Starting state from file: %s" % self.sio_db.file_state
        except IOError:
            self.sio_db = SioFileStateInit()

    def save(self):
        """
        Save the sio db using pickle to store the object
        """
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tempfn = os.tempnam(mdd_config.data_path, 'sio.')
        pickle.dump(self.sio_db, open(tempfn, 'w'))
        os.rename(tempfn, sio_db_file)

    def get_file_state(self, filename):
        """
        Get the file state for the specified file name
        :param filename: The file name to return the state for
        :return: The file state dictionary
        """
        if filename in self.sio_db.file_state:
            return self.sio_db.file_state.get(filename)
        return None

    def init_file_state(self, filename):
        """
        Initialize the file state to the default state dictionary
        :param filename: The filename to initialize the state for
        :return: the initialized file state dictionary
        """
        self.sio_db.file_state[filename] = {StateKey.UNPROCESSED_DATA: None,
                                            StateKey.FILE_SIZE: 0,
                                            StateKey.OUTPUT_INDEX: 0}
        return self.sio_db.file_state.get(filename)


class SioParse(object):
    def __init__(self):
        # initialize the object used to store the sio parser state
        self.sio_db = SioState()

    def parse_file(self, file_name):
        """
        Find any complete sio blocks in input file and copy them to their respective output files
        :param file_name: The input file name to parse
        """
        # get the current file state from the dictionary, or initialize
        # it if it doesn't exist
        file_state = self.sio_db.get_file_state(file_name)
        if file_state is None:
            file_state = self.sio_db.init_file_state(file_name)

        # insert the file index at the end of the file name before the extension
        file_out_start = file_name[:-4] + '_' + str(file_state[StateKey.OUTPUT_INDEX])
        file_out_end = file_name[-4:]

        # get the full input file path and find the current file size
        full_path_in = mdd_config.datafile(file_name)
        file_len = os.stat(full_path_in).st_size

        # update the file size and unprocessed data based on the input file length
        if file_state[StateKey.UNPROCESSED_DATA] is None:
            file_state[StateKey.UNPROCESSED_DATA] = [[0, file_len]]
            file_state[StateKey.FILE_SIZE] = file_len
        else:
            self.update_state_file_length(file_state, file_len)

        # increment output index each time we read this file
        file_state[StateKey.OUTPUT_INDEX] += 1

        fid_in = open(full_path_in, 'rb')

        newly_processed_blocks = []
        # loop over unprocessed blocks
        for unproc in file_state[StateKey.UNPROCESSED_DATA]:
            # read the next unprocessed data block from the file
            fid_in.seek(unproc[START_IDX])
            block_len = unproc[END_IDX] - unproc[START_IDX]
            data_block = fid_in.read(block_len)

            # loop and find each sio header in this unprocessed block
            for match in SIO_HEADER_MATCHER.finditer(data_block):

                # get the file string associated with this instrument ID from the sio header
                file_type = ID_MAP.get(match.group(SIO_HEADER_GROUP_ID))
                # insert the file type into the file name
                full_path_out = mdd_config.datafile(file_out_start + '.' + file_type + file_out_end)

                # open the output file in append mode, creating if it doesn't exist
                fid_out = open(full_path_out, 'a+')

                # get length of data packet carried within this sio header
                data_len = int(match.group(SIO_HEADER_GROUP_DATA_LENGTH), 16)
                # end index relative to the unprocessed block
                end_block_idx = match.end(0) + data_len + 1
                # end index relative to the match
                end_match_idx = SIO_HEADER_LENGTH - 1 + data_len

                match_block = data_block[match.start(0):end_block_idx]
                orig_len = len(match_block)
                # replace escape modem chars
                match_block = match_block.replace(b'\x18\x6b', b'\x2b')
                match_block = match_block.replace(b'\x18\x58', b'\x18')
                # store how many chars were replaced in this block for updating the state
                n_replaced = orig_len - len(match_block)
                # need to increase block length if replaced characters to include the rest of the block
                match_block += data_block[end_block_idx:end_block_idx + n_replaced]

                if end_match_idx < len(match_block) and match_block[end_match_idx] == SIO_BLOCK_END:
                    # found the matching end of the packet, this block is complete,
                    # write it to output file
                    fid_out.write(match_block[:end_match_idx + 1])

                    # adjust the start and end indices to be relative to the file rather than the block
                    start_file_idx = match.start(0) + unproc[START_IDX]
                    end_file_idx = end_block_idx + n_replaced + unproc[START_IDX]
                    newly_processed_blocks.append([start_file_idx, end_file_idx])

                fid_out.close()

        # pre combine blocks so there aren't so many to loop over
        newly_processed_blocks = SioParse._combine_adjacent_packets(newly_processed_blocks)

        # remove the processed blocks from the unprocessed file state
        for new_block in newly_processed_blocks:
            self.update_processed_file_state(file_state, new_block[START_IDX], new_block[END_IDX])

        fid_in.close()

    def save(self):
        """
        Trigger the sio database to be saved
        """
        self.sio_db.save()

    def update_state_file_length(self, file_state, file_len):
        """
        Update the file state based on any changes in the file length, appending or
         changing the last unprocessed data indices to include any appended file data.
        :param file_state: Current file state
        :param file_len: Length of file
        """
        last_size = file_state[StateKey.FILE_SIZE]
        if file_state[StateKey.UNPROCESSED_DATA] == [] and last_size < file_len:
            # we have processed up to the last file size, append a new block that
            # goes from the last file size to the new file size
            file_state[StateKey.UNPROCESSED_DATA].append([last_size, file_len])
            file_state[StateKey.FILE_SIZE] = file_len

        elif file_state[StateKey.UNPROCESSED_DATA] != [] and \
                file_state[StateKey.UNPROCESSED_DATA][-1][END_IDX] < file_len:

            if last_size > file_state[StateKey.UNPROCESSED_DATA][-1][END_IDX]:
                # the previous file size is greater than the last unprocessed index so
                # we have processed up to the last file size, append a new block
                # that goes from the last file size to the new file size
                file_state[StateKey.UNPROCESSED_DATA].append(last_size, file_len)
                file_state[StateKey.FILE_SIZE] = file_len

            elif last_size == file_state[StateKey.UNPROCESSED_DATA][-1][END_IDX]:
                # if the last unprocessed is the last file size, just increase the last index
                file_state[StateKey.UNPROCESSED_DATA][-1][1] = file_len
                file_state[StateKey.FILE_SIZE] = file_len

    def update_processed_file_state(self, file_state, start_idx, end_idx):
        """
        Update the file state dictionary after the block starting at start idx
        and ending at end idx has been processed
        :param file_state: current file state
        :param start_idx: start of processed block
        :param end_idx: end of processed block
        """
        for unproc in file_state[StateKey.UNPROCESSED_DATA]:

            if start_idx >= unproc[START_IDX] and end_idx <= unproc[END_IDX]:
                # packet is within this unprocessed data, remove it
                file_state[StateKey.UNPROCESSED_DATA].remove(unproc)

                # add back any data still unprocessed on either side
                if start_idx > unproc[START_IDX]:
                    file_state[StateKey.UNPROCESSED_DATA].append([unproc[START_IDX], start_idx])
                if end_idx < unproc[END_IDX]:
                    file_state[StateKey.UNPROCESSED_DATA].append([end_idx, unproc[END_IDX]])

                # once we have found which unprocessed section this packet is in,
                # move on to next packet
                break

        file_state[StateKey.UNPROCESSED_DATA] = sorted(file_state[StateKey.UNPROCESSED_DATA])
        file_state[StateKey.UNPROCESSED_DATA] = SioParse._combine_adjacent_packets(
            file_state[StateKey.UNPROCESSED_DATA])

    @staticmethod
    def _combine_adjacent_packets(packets):
        """
        Combine packets which are adjacent and have the same start/end into one packet
        i.e [[a,b], [b,c]] -> [[a,c]]
        :param packets An array of packets, with the form [[start, end], [next_start, next_end], ...]
        :retval A new array of packets where adjacent packets will have their indices combined into one
        """
        combined_packets = []
        idx = 0
        while idx < len(packets):
            start_idx = packets[idx][START_IDX]
            # loop until the end of this packet doesn't equal the start of the following packet
            next_inc = 0
            while idx + next_inc + 1 < len(packets) and \
                    packets[idx + next_inc][END_IDX] == packets[idx + next_inc + 1][START_IDX]:
                next_inc += 1

            end_idx = packets[idx + next_inc][END_IDX]
            # append the new combined packet indices
            combined_packets.append([start_idx, end_idx])
            idx = idx + next_inc + 1
        return combined_packets
