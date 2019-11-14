from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class RelAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_REL

    @staticmethod
    def getLevels():
        return {
                "Householder"                                                                                              : [0],
                "Husband/Wife"                                                                                             : [1],
                "Biological Son/Daughter"                                                                                  : [2],
                "Adopted Son/Daughter"                                                                                     : [3],
                "Stepson/Stepdaughter"                                                                                     : [4],
                "Brother/Sister"                                                                                           : [5],
                "Father/Mother"                                                                                            : [6],
                "Grandchild"                                                                                               : [7],
                "Parent-in-law"                                                                                            : [8],
                "Son/Daughter-in-law"                                                                                      : [9],
                "Other relative"                                                                                           : [10],
                "Roomer, Boarder"                                                                                          : [11],
                "Housemate, Roommate"                                                                                      : [12],
                "Unmarried Partner"                                                                                        : [13],
                "Other Non-relative"                                                                                       : [14],
                "Federal Detention Centers (101)"                                                                          : [15],
                "Federal Prisons (102)"                                                                                    : [16],
                "State Prisons (103)"                                                                                      : [17],
                "Local Jails and Other Municipal Confinement Facilities (104)"                                             : [18],
                "Correctional Residential Facilities (105)"                                                                : [19],
                "Military Disciplinary Barracks and Jails (106)"                                                           : [20],
                "Group Homes for Juveniles (Non-Correctional) (201)"                                                       : [21],
                "Residential Treatment Centers (Non-Correctional) (202)"                                                   : [22],
                "Correctional Facilities Intended for Juveniles (203)"                                                     : [23],
                "Nursing Facilities/Skilled-Nursing Facilities (301)"                                                      : [24],
                "Mental (Psychiatric) Hospitals and Psychiatric Units in Other Hospitals (401)"                            : [25],
                "Hospitals with Patients Who Have No Usual Home Elsewhere (402)"                                           : [26],
                "In-Patient Hospice Facilities (403)"                                                                      : [27],
                "Military Treatment Facilities with Assigned Patients (404)"                                               : [28],
                "Residential Schools for People with Disabilities (405)"                                                   : [29],
                "College/University Student Housing (501)"                                                                 : [30],
                "Military Quarters (601)"                                                                                  : [31],
                "Military Ships (602)"                                                                                     : [32],
                "Emergency and Transitional Shelters (with Sleeping Facilities) for People Experiencing Homelessness (701)": [33],
                "Soup Kitchens (702)"                                                                                      : [34],
                "Regularly Scheduled Mobile Food Vans (704)"                                                               : [35],
                "Targeted Non-Sheltered Outdoor Locations (706)"                                                           : [36],
                "Group Homes Intended for Adults (801)"                                                                    : [37],
                "Residential Treatment Centers for Adults (802)"                                                           : [38],
                "Maritime/Merchant Vessels (900)"                                                                          : [39],
                "Workers' Group Living Quarters and Job Corps Centers (901)"                                               : [40],
                "Living Quarters for Victims of Natural Disasters (903)"                                                   : [41],
                "Religious Group Quarters and Domestic Violence Shelters (904)"                                            : [42]
            }

    #TODO recodes