#!/usr/bin/env python
import logging
from collections import OrderedDict

import time

from uframe import SensorInventory, AsyncFetcher, log


def get_glider_streams(inventory):
    subsites = ['CP05MOAS', 'CE05MOAS']
    nodes = ['GL388', 'GL384']
    methods = ['recovered_host']
    target_streams = ['adcp_velocity_glider', 'dosta_abcdjm_glider_recovered', 'parad_m_glider_recovered',
                      'ctdgv_m_glider_instrument_recovered', 'flort_m_glider_recovered']
    streams = [stream for stream in inventory.get_streams(subsites=subsites, nodes=nodes, methods=methods)
               if stream.stream in target_streams]
    return streams


def get_site_streams(inventory):
    subsites = ['CP02PMUI']
    methods = ['recovered_host', 'recovered_wfp']
    target_streams = ['mopak_o_dcl_accel_recovered', 'ctdpf_ckl_wfp_instrument_recovered', 'vel3d_k_wfp_instrument',
                      'dofst_k_wfp_instrument_recovered', 'parad_k__stc_imodem_instrument_recovered',
                      'flort_kn_stc_imodem_instrument_recovered']
    streams = [stream for stream in inventory.get_streams(subsites=subsites, methods=methods)
               if stream.stream in target_streams]
    return streams


def get_cabled_streams(inventory):
    subsites = ['RS01SLBS']
    methods = ['streamed']
    target_streams = ['vel3d_b_sample', 'prest_real_time', 'adcp_velocity_beam', 'ctdpf_optode_sample',
                      'horizontal_electric_field', 'optaa_sample']
    streams = [stream for stream in inventory.get_streams(subsites=subsites, methods=methods)
               if stream.stream in target_streams]
    return streams


def main():
    inv = SensorInventory()
    streams = get_glider_streams(inv) + get_site_streams(inv) + get_cabled_streams(inv)
    active = OrderedDict()
    complete = []
    index = 0

    while True:
        try:
            while len(active) < len(streams):
                stream = streams[index]
                if stream in active:
                    # we're still processing the last instance of this stream
                    # don't add any more jobs until this one is finished
                    break
                fetcher = active[stream] = AsyncFetcher(stream)
                fetcher.query()
                index = (index + 1) % len(streams)

            for stream in active.keys():
                fetcher = active[stream]
                if fetcher.check_status():
                    complete.append(fetcher)
                    del active[stream]
                log.info(fetcher)

            log.info('sleep: %d running %d complete', len(active), len(complete))
            time.sleep(1)

        except KeyboardInterrupt:
            break

    for fetcher in complete:
        log.info(fetcher)

main()
