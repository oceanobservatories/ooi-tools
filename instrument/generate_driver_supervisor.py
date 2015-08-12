#!/usr/bin/env python
"""
generate_driver_supervisor.py

Generate a supervisord file from CSV input

Usage:
    generate_driver_supervisor.py <csv_file>
"""
import jinja2
import docopt
from csv import DictReader



instruments = {
                'adcps': {'module':'mi.instrument.teledyne.workhorse.adcp.driver',         'klass':'InstrumentDriver'},
                'adcpt': {  'module':'mi.instrument.teledyne.workhorse.adcp.driver',         'klass':'InstrumentDriver'},
                'zplsc': {   'module':'mi.instrument.kut.ek60.ooicore.driver',                'klass':'InstrumentDriver'},
                'velpt': {  'module':'mi.instrument.nortek.aquadopp.ooicore.driver',         'klass':'InstrumentDriver'},
                'vel3dc': { 'module':'mi.instrument.nortek.vector.ooicore.driver',           'klass':'InstrumentDriver'},
                'vel3db': { 'module':'mi.instrument.nobska.mavs4.ooicore.driver',            'klass':'mavs4InstrumentDriver'},
                'vadcp': {  'module':'mi.instrument.teledyne.workhorse.vadcp.driver',        'klass':'InstrumentDriver'},
                'vadcp_5': {'module':'mi.instrument.teledyne.workhorse.vadcp.driver',        'klass':'InstrumentDriver'},
                'trhph': {  'module':'mi.instrument.uw.bars.ooicore.driver',                 'klass':'InstrumentDriver'},
                'tmpsf': {   'module':'mi.instrument.rbr.xr_420_thermistor_24.ooicore.driver','klass':'InstrumentDriver'},
                'thsph': {   'module':'mi.instrument.um.thsph.ooicore.driver',                'klass':'InstrumentDriver'},
                'spkir': {   'module':'mi.instrument.satlantic.ocr_507_icsw.ooicore.driver',  'klass':'SatlanticOCR507InstrumentDriver'},
                'prest': {   'module':'mi.instrument.seabird.sbe54tps.driver',                'klass':'SBE54PlusInstrumentDriver'},
                'camds': {  'module':'mi.instrument.kml.cam.camds.driver',                   'klass':'InstrumentDriver'},
                'botpt': {   'module':'mi.instrument.noaa.botpt.ooicore.driver',              'klass':'InstrumentDriver'},
                'ctdbp': {   'module':'mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver',   'klass':'InstrumentDriver'},
                'ctdpfa': {  'module':'mi.instrument.seabird.sbe16plus_v2.ctdpf_sbe43.driver','klass':'InstrumentDriver'},
                'ctdpfb': {  'module':'mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver',   'klass':'InstrumentDriver'},
                'd1000': {   'module':'mi.instrument.mclane.ras.d1000.driver',                'klass':'InstrumentDriver'},
                'flord': {  'module':'mi.instrument.wetlabs.fluorometer.flort_d.driver',     'klass':'InstrumentDriver'},
                'flort': {  'module':'mi.instrument.wetlabs.fluorometer.flort_d.driver',     'klass':'InstrumentDriver'},
                'hpies': {  'module':'mi.instrument.uw.hpies.ooicore.driver',                'klass':'InstrumentDriver'},
                'massp-mcu': { 'module':'mi.instrument.harvard.massp.ooicore.driver',        'klass':'InstrumentDriver'},
                'massp-rga': { 'module':'mi.instrument.harvard.massp.ooicore.driver',        'klass':'InstrumentDriver'},
                'massp-turbo': { 'module':'mi.instrument.harvard.massp.ooicore.driver',      'klass':'InstrumentDriver'},
                'nutnr': {   'module':'mi.instrument.satlantic.suna_deep.ooicore.driver',     'klass':'InstrumentDriver'},
                'optaa': {   'module':'mi.instrument.wetlabs.ac_s.ooicore.driver',            'klass':'InstrumentDriver'},
                'parad': {   'module':'mi.instrument.satlantic.par_ser_600m.ooicore.driver',  'klass':'InstrumentDriver'},
                'pco2wa': { 'module':'mi.instrument.sunburst.sami2_pco2.pco2a.driver',       'klass':'InstrumentDriver'},
                'pco2wb': {  'module':'mi.instrument.sunburst.sami2_pco2.pco2b.driver',       'klass':'InstrumentDriver'},
                'phsen': {   'module':'mi.instrument.sunburst.sami2_ph.ooicore.driver',       'klass':'InstrumentDriver'},
                'ppsdn': {   'module':'mi.instrument.mclane.ras.ppsdn.driver',                'klass':'InstrumentDriver'},
                'rasfl': {  'module':'mi.instrument.mclane.ras.rasfl.driver',                'klass':'InstrumentDriver'}
}

loader = jinja2.FileSystemLoader(searchpath="templates")
env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
super_template = env.get_template('driver_supervisor.jinja')


def create_tc_dict(csv_file):
    tc_dict = {}
    with open(csv_file) as fh:
        for row in DictReader(fh):
            name = create_tc_name(row)
            inst_name = row['instrument']
            if name is not None:
                tc_dict[name] = row
                tc_dict[name].update(instruments[inst_name])
    return tc_dict


def create_tc_name(tc_dict):
    if any([tc_dict['instrument'] == '', tc_dict['refdes'] == '']):
        return None
    return '%s' % ( tc_dict['refdes'])


def create_supervisord_config(tc_dict):
    groups = {}
    for name in tc_dict:
        each = tc_dict[name]
        group = each['group']
        groups.setdefault(group, []).append(each)
    return super_template.render(groups=groups)


def write(tc_configs):
    with open(('drivers.conf'), 'wb') as fh:
        fh.write(tc_configs)


def main():
    options = docopt.docopt(__doc__)
    csv_file = options['<csv_file>']
    tc_dict = create_tc_dict(csv_file)
    supervisord_config = create_supervisord_config(tc_dict)
    write(supervisord_config)


if __name__ == '__main__':
    main()

