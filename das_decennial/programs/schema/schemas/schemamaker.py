import programs.schema.schema as sk
from constants import CC
from programs.schema.attributes.age import AgeAttr
from programs.schema.attributes.cenrace import CenraceAttr
from programs.schema.attributes.citizen import CitizenAttr
from programs.schema.attributes.hhgq import HHGQAttr
from programs.schema.attributes.hisp import HispAttr
from programs.schema.attributes.rel import RelAttr
from programs.schema.attributes.sex import SexAttr
from programs.schema.attributes.hhage import HHAgeAttr
from programs.schema.attributes.hhelderly import HHElderlyAttr
from programs.schema.attributes.hhhisp import HHHispAttr
from programs.schema.attributes.hhmulti import HHMultiAttr
from programs.schema.attributes.hhrace import HHRaceAttr
from programs.schema.attributes.hhsex import HHSexAttr
from programs.schema.attributes.hhsize import HHSizeAttr
from programs.schema.attributes.hhtype import HHTypeAttr
from programs.schema.attributes.hhgq1940 import HHGQ1940Attr as HHGQ_1940
from programs.schema.attributes.age1940 import Age1940Attr as AGE_1940
from programs.schema.attributes.sex1940 import Sex1940Attr as SEX_1940
from programs.schema.attributes.hisp1940 import Hispanic1940Attr as HISPANIC_1940
from programs.schema.attributes.citizen1940 import Citizen1940Attr as CITIZEN_1940
from programs.schema.attributes.race1940 import Race1940Attr as RACE_1940

_schema_dict = {
    CC.SCHEMA_REDUCED_DHCP_HHGQ: [HHGQAttr, SexAttr, AgeAttr, HispAttr, CenraceAttr, CitizenAttr],
    CC.SCHEMA_SF1: [RelAttr, SexAttr, AgeAttr, HispAttr, CenraceAttr],
    CC.SCHEMA_HOUSEHOLD2010: [HHSexAttr, HHAgeAttr, HHHispAttr, HHRaceAttr, HHSizeAttr, HHTypeAttr, HHElderlyAttr, HHMultiAttr],
    CC.DAS_1940: [HHGQ_1940, SEX_1940, AGE_1940, HISPANIC_1940, RACE_1940, CITIZEN_1940]
}

def buildSchema(path=None, name=CC.SCHEMA_REDUCED_DHCP_HHGQ):
    return SchemaMaker.fromName(name, path)

class SchemaMaker:

    @staticmethod
    def fromName(name, path=None):
        return SchemaMaker.fromAttlist(name, _schema_dict[name], path)

    @staticmethod
    def fromAttlist(name, attlist, path=None):
        # prepare schema information
        dimnames = [att.getName() for att in attlist]
        shape = tuple(att.getSize() for att in attlist)
        leveldict = { att.getName():att.getLevels() for att in attlist}
        levels = sk.unravelLevels(leveldict)

        # construct schema object
        myschema = sk.Schema(name, dimnames, shape, recodes=None, levels=levels)

        for att in attlist:
            att_recodes = att.getRecodes()
            for myrecode in att_recodes:
                myschema.addRecode(*myrecode)

        ###############################################################################
        # Save Schema (as a JSON file)
        ###############################################################################
        if path is not None:
            myschema.saveAsJSON(path)
        return myschema
