"""
File used with nosetest to test the sio unpacking code, which was
added to the previously written .mdd parsing into nodeXXp1.dat files.
Usage: nosetests test_sio_unpack.py
"""
__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import unittest
import os
import mdd
import pickle
import glob
import time

from sio_unpack import SIO_HEADER_MATCHER, SIO_HEADER_GROUP_DATA_LENGTH, \
    SIO_HEADER_GROUP_ID, SIO_BLOCK_END, StateKey


INPUT_HYPM_PATH = 'gp02hypm_mdd'  # deployment 1 .mdd files
INPUT_FLMB_PATH = 'gp03flmb_mdd'  # deployment 1 .mdd files
OUTPUT_PATH = 'data'


class TestSioUnpack(unittest.TestCase):

    def setUp(self):

        # create the output path if it doesn't exist
        if not os.path.exists(OUTPUT_PATH):
            os.mkdir(OUTPUT_PATH)

        # remove all generated files
        pckl_files = glob.glob(OUTPUT_PATH + '/*.pckl')
        for pckl_file in pckl_files:
            os.remove(pckl_file)

        node_files = glob.glob(OUTPUT_PATH + '/node*.dat')
        for node_file in node_files:
            os.remove(node_file)

    def test_simple(self):
        """
        Run a simple test which parses two .mdd files into a node file and its
        individual instrument group files.  Confirm the data types in each
        individual file only contain the allowed IDs.
        """
        # blocks [0 3583] [3840 4058]
        test_file1 = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-206-2-0.mdd')
        # blocks [0 1279] [1536 1791] [2048 2303] [2560 2815] [3072 4059]
        test_file2 = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-206-3-0.mdd')

        # parse the two test files into the node and instrument group files
        mdd.procall([test_file1, test_file2])

        data_orig = self.read_full_file('node58p1.dat')

        # read the data from all generated files into one data string
        data_out = self.read_full_file('node58p1_0.status.dat')
        # confirm this file only has the allowed instrument IDs
        self.check_sio_type(data_out, ['PS', 'CS'])

        data_out_wa = self.read_full_file('node58p1_0.wa_wfp.dat')
        # confirm this file only has the allowed instrument IDs
        self.check_sio_type(data_out_wa, ['WA'])
        data_out += data_out_wa

        data_out_wc = self.read_full_file('node58p1_0.wc_wfp.dat')
        # confirm this file only has the allowed instrument IDs
        self.check_sio_type(data_out_wc, ['WC'])
        data_out += data_out_wc

        data_out_we = self.read_full_file('node58p1_0.we_wfp.dat')
        # confirm this file only has the allowed instrument IDs
        self.check_sio_type(data_out_wc, ['WE'])
        data_out += data_out_we

        # confirm that all data blocks from the node data file made it
        # into the instrument group files
        if not TestSioUnpack.compare_sio_matches(data_orig, data_out):
            self.fail("Failed sio block compare")

    def test_state(self):
        """
        Parse two files, check that the state saved in the pickle file matches the expected,
        then parse another file and check that the state updated correctly.
        """
        # blocks [0 3583] [3840 4058]
        test_file1 = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-206-2-0.mdd')
        # blocks [0 1279] [1536 1791] [2048 2303] [2560 2815] [3072 4059]
        test_file2 = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-206-3-0.mdd')

        # parse the two .mdd files into the node and instrument group files
        mdd.procall([test_file1, test_file2])

        file_state = self.get_file_state('node58p1.dat')
        # there is an unprocessed '/n' in between records
        expected_file_state = {StateKey.UNPROCESSED_DATA: [[4059, 4060]],
                               StateKey.FILE_SIZE: 4060,
                               StateKey.OUTPUT_INDEX: 1}

        if file_state != expected_file_state:
            print file_state
            self.fail("Expected file state 1 does not match")

        # blocks [0 2047] [2304 4095] [4096 7451]
        test_file3 = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-206-6-0.mdd')

        # parse another .mdd file adding on to the node file, and making
        # another sequence of instrument group files
        mdd.procall([test_file3])

        file_state = self.get_file_state('node58p1.dat')
        expected_file_state = {StateKey.UNPROCESSED_DATA: [[4059, 4060]],
                               StateKey.FILE_SIZE: 7452,
                               StateKey.OUTPUT_INDEX: 2}

        if file_state != expected_file_state:
            print "file state: '%s'" % file_state
            self.fail("Expected file state 2 does not match")

        data_orig = self.read_full_file('node58p1.dat')

        # read the data from all generated files into one data string
        data_out = self.read_full_file('node58p1_0.status.dat')
        data_out += self.read_full_file('node58p1_0.wa_wfp.dat')
        data_out += self.read_full_file('node58p1_0.wc_wfp.dat')
        data_out += self.read_full_file('node58p1_0.we_wfp.dat')
        data_out += self.read_full_file('node58p1_1.status.dat')
        data_out += self.read_full_file('node58p1_1.wa_wfp.dat')
        data_out += self.read_full_file('node58p1_1.wc_wfp.dat')
        data_out += self.read_full_file('node58p1_1.we_wfp.dat')

        # confirm data in the node file matches those output in the instrument groups
        if not TestSioUnpack.compare_sio_matches(data_orig, data_out):
            self.fail("Failed sio block compare")

    def test_large_hypm(self):
        """
        Test with a larger set of hypm files
        """
        test_files_225 = glob.glob(INPUT_HYPM_PATH + '/unit_364-2013-225*.mdd')
        mdd.procall(test_files_225)
        # compare the node58p1 data and that in the 1st sequence of instrument group files
        data_out = self.compare_node58()

        # test with a second set of files
        test_files_237 = glob.glob(INPUT_HYPM_PATH + '/unit_364-2013-237*.mdd')
        mdd.procall(test_files_237)

        # compare the node58p1 data and that in the 2nd sequence of instrument group files
        self.compare_node58(1, data_out)

    def test_full_hypm(self):
        """
        Test with all the hypm files
        """
        test_files = glob.glob(INPUT_HYPM_PATH + '/*.mdd')

        mdd.procall(test_files)

        self.compare_node58()

    def test_large_flmb(self):
        """
        Test with a larger set of flmb files, confirming that the instrument files generated only contain the
        allowed instrument IDs
        """
        test_files_218 = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-218*.mdd')

        mdd.procall(test_files_218)

        data_orig = self.read_full_file('node59p1.dat')

        data_out = self.read_full_file('node59p1_0.status.dat')
        self.check_sio_type(data_out, ['CS', 'PS'])

        data_adcps = self.read_full_file('node59p1_0.adcps.dat')
        self.check_sio_type(data_adcps, ['AD'])
        data_out += data_adcps

        data_ctdmo = self.read_full_file('node59p1_0.ctdmo.dat')
        self.check_sio_type(data_ctdmo, ['CT', 'CO'])
        data_out += data_ctdmo

        data_dosta = self.read_full_file('node59p1_0.dosta.dat')
        self.check_sio_type(data_dosta, ['DO'])
        data_out += data_dosta

        data_flort = self.read_full_file('node59p1_0.flort.dat')
        self.check_sio_type(data_flort, ['FL'])
        data_out += data_flort

        data_phsen = self.read_full_file('node59p1_0.phsen.dat')
        self.check_sio_type(data_phsen, ['PH'])
        data_out += data_phsen

        if not TestSioUnpack.compare_sio_matches(data_orig, data_out):
            self.fail("Failed sio block compare")

        test_files = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-205*.mdd')
        test_files_217 = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-217*.mdd')
        test_files_219 = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-219*.mdd')

        test_files.extend(test_files_217)
        test_files.extend(test_files_219)

        mdd.procall(test_files)

        data_out = self.compare_node59(1, data_out)

        test_files = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-233*.mdd')
        test_files_231 = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-231*.mdd')

        test_files.extend(test_files_231)

        mdd.procall(test_files)

        self.compare_node59(2, data_out)

    def test_hypm_flmb(self):
        """
        Test with data in two different locations at the same time
        """
        # test with two different locations at the same time
        test_files = glob.glob(INPUT_FLMB_PATH + '/unit_363-2013-218*.mdd')
        test_files_225 = glob.glob(INPUT_HYPM_PATH + '/unit_364-2013-225*.mdd')

        test_files.extend(test_files_225)

        mdd.procall(test_files)

        # this one can take a while to process all the files, sleep for a bit to
        # make sure it is done before checking the output files
        time.sleep(3)

        self.compare_node58()
        self.compare_node59()

    def test_duplicate(self):
        """
        Test to fix duplicates in output
        """
        test_file = os.path.join(INPUT_HYPM_PATH, 'unit_364-2013-225-1-0.mdd')

        mdd.procall([test_file])
     
        self.compare_node58()

    def test_sects(self):
        """
        Test that a processing done in the getmdd script succeeds, since we don't have enough config to run the script
        """

        test_files = glob.glob(INPUT_HYPM_PATH + '/*.mdd')
        test_files.extend(glob.glob(INPUT_FLMB_PATH + '/*.mdd'))

        sects = mdd.procall(test_files)

        TestSioUnpack.latest(sects)

    def compare_node58(self, index=0, data_in=None):
        """
        Compare node58 port 1 output and generated instrument files (hypm)
        @param index - the index of the sequence of the instrument files to check
        @param data_in - if a sequence is greater than 1, since we are comparing the entire file need to include
                         data from previous sequence
        """
        data_orig = self.read_full_file('node58p1.dat')

        if data_in is not None:
            data_out = data_in
        else:
            data_out = '' 
        data_out += self.read_full_file('node58p1_' + str(index) + '.status.dat')
        data_out += self.read_full_file('node58p1_' + str(index) + '.wa_wfp.dat')
        data_out += self.read_full_file('node58p1_' + str(index) + '.wc_wfp.dat')
        data_out += self.read_full_file('node58p1_' + str(index) + '.we_wfp.dat')

        if not TestSioUnpack.compare_sio_matches(data_orig, data_out):
            self.fail("Failed sio block compare")
        return data_out

    def compare_node59(self, index=0, data_in=None):
        """
        Compare node 59 port 1 output and generated instrument files (flmb)
        @param index - the index of the sequence of the instrument files to check
        @param data_in - if a sequence is greater than 1, since we are comparing the entire file need to include
                         data from previous sequence
        """
        data_orig = self.read_full_file('node59p1.dat')

        if data_in is not None:
            data_out = data_in
        else:
            data_out = ''
        # append new set of data to the original
        data_out += self.read_full_file('node59p1_' + str(index) + '.status.dat')
        data_out += self.read_full_file('node59p1_' + str(index) + '.adcps.dat')
        data_out += self.read_full_file('node59p1_' + str(index) + '.ctdmo.dat')
        data_out += self.read_full_file('node59p1_' + str(index) + '.dosta.dat')
        data_out += self.read_full_file('node59p1_' + str(index) + '.flort.dat')
        data_out += self.read_full_file('node59p1_' + str(index) + '.phsen.dat')

        if not TestSioUnpack.compare_sio_matches(data_orig, data_out):
            self.fail("Failed sio block compare")

        return data_out

    def read_full_file(self, filename):
        """
        Read and return the entire file's data
        :param filename - the filename to read
        :return: file data
        """
        output_file = os.path.join(OUTPUT_PATH, filename)
        fid = open(output_file, 'rb')
        data_out = fid.read()
        fid.close()
        return data_out

    def get_file_state(self, filename):
        """
        Get the file state for this filename from the stored sio pickle file
        :param filename:
        :return: file state dictionary
        """
        # load the pickle file
        pkl_fid = open(OUTPUT_PATH + '/sio.pckl')
        sio_db = pickle.load(pkl_fid)
        pkl_fid.close()

        # get the file state for this filename
        return sio_db.file_state.get(filename)

    @staticmethod
    def compare_sio_matches(data_orig, data_out):
        """
        Compare if all sio matching blocks of data from the original set are found in the output data
        :param data_orig: Original block of sio data
        :param data_out: Output data
        :return: True if all sio blocks from the original data are found in output data, false if not
        """
        for match in SIO_HEADER_MATCHER.finditer(data_orig):
            data_len = int(match.group(SIO_HEADER_GROUP_DATA_LENGTH), 16)
            end_packet_idx = match.end(0) + data_len

            # need to perform escape modem character replace to match data
            orig_packet = data_orig[match.start(0):end_packet_idx + 1]
            n_orig = len(orig_packet)
            data_repl = orig_packet.replace(b'\x18\x6b', b'\x2b')
            data_repl = data_repl.replace(b'\x18\x58', b'\x18')
            n_replace = n_orig - len(data_repl)
            data_repl += data_orig[end_packet_idx + 1: end_packet_idx + 1 + n_replace]
            if data_repl[-1] == SIO_BLOCK_END:
                if data_out.find(data_repl) == -1:
                    print "data block from original file not found in output %s" % \
                          data_orig[match.start(0) + 1:match.start(0) + 32]
                    return False
                else:
                    orig_count = data_orig.count(orig_packet)
                    new_count = data_out.count(data_repl)
                    if orig_count != 1:
                        print "data %s in original file %d times" % \
                              (orig_packet[1:32], orig_count)
                    if new_count != orig_count:
                        print "data %s duplicated in output %d times" % \
                            (data_orig[match.start(0) + 1:match.start(0) + 32], new_count)
                        return False

            else:
                print "skipping incomplete block %s" % data_orig[match.start(0) + 1:match.start(0) + 32]

        return True

    @staticmethod
    def check_sio_type(data_out, ids):
        """
        Check that the file contains only sio IDs that it should
        :param data_out: Data from output file
        :param ids: Array of valid IDs
        :return: True if all matches, False if any not match
        """
        for match in SIO_HEADER_MATCHER.finditer(data_out):
            if match.group(SIO_HEADER_GROUP_ID) not in ids:
                return False
        return True

    @staticmethod
    def latest(sects):
        # this method is similar to latest in getmdd, but since we don't have all the config needed to run that
        # make a function that processes the output of mdd in the same way to make sure this passes
        nodes = {}
        # Get the most recent, highest known offset for each node, port 1
        for sect in sects:
            if sect.port != 1:
                continue
            snode = sect.node
            if snode not in nodes:
                nodes[snode] = [sect.end, sect.time]
            else:
                if sect.end > nodes[snode][0]:
                    nodes[snode][0] = sect.end
                if sect.time > nodes[snode][1]:
                    nodes[snode][1] = sect.end
                    nodes[snode][1] = sect.time
        print nodes
