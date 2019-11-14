import programs.schema.schema
import constants as con

###############################################################################
# Table module imports and dictionary
###############################################################################
import programs.schema.table_building.building_PL94_tables as Table_PL94
import programs.schema.table_building.building_PL94_P12_tables as Table_PL94_P12
import programs.schema.table_building.building_PL94_CVAP_tables as Table_PL94_CVAP
import programs.schema.table_building.building_SF1_tables as Table_SF1
import programs.schema.table_building.building_Household2010_tables as Table_Household2010
import programs.schema.table_building.building_DHCP_HHGQ_tables as Table_DHCP_HHGQ


###############################################################################
# Schema module imports and dictionary
###############################################################################
import programs.schema.schemas.Schema_PL94 as Schema_PL94
import programs.schema.schemas.Schema_PL94_CVAP as Schema_PL94_CVAP
import programs.schema.schemas.Schema_PL94_P12 as Schema_PL94_P12
import programs.schema.schemas.Schema_1940 as Schema_1940
import programs.schema.schemas.Schema_SF1 as Schema_SF1
import programs.schema.schemas.Schema_DHCP_HHGQ as Schema_DHCP_HHGQ
import programs.schema.schemas.Schema_Household2010 as Schema_Household2010
import programs.schema.schemas.Schema_TenUnit2010 as Schema_TenUnit2010
import programs.schema.schemas.schemamaker as schemamaker
###############################################################################
# Invariant module imports and dictionary
###############################################################################
import programs.invariants.Invariants_PL94 as Invariants_PL94
import programs.invariants.Invariants_PL94_CVAP as Invariants_PL94_CVAP
import programs.invariants.Invariants_PL94_P12 as Invariants_PL94_P12
import programs.invariants.Invariants_1940 as Invariants_1940
import programs.invariants.Invariants_SF1 as Invariants_SF1
import programs.invariants.Invariants_DHCP_HHGQ as Invariants_DHCP_HHGQ
import programs.invariants.Invariants_Household2010 as Invariants_Household2010
import programs.invariants.Invariants_TenUnit2010 as Invariants_TenUnit2010

###############################################################################
# Constraint module imports and dictionary
###############################################################################
import programs.constraints.Constraints_PL94 as Constraints_PL94
import programs.constraints.Constraints_PL94_CVAP as Constraints_PL94_CVAP
import programs.constraints.Constraints_PL94_P12 as Constraints_PL94_P12
import programs.constraints.Constraints_1940 as Constraints_1940
import programs.constraints.Constraints_SF1 as Constraints_SF1
import programs.constraints.Constraints_DHCP_HHGQ as Constraints_DHCP_HHGQ
import programs.constraints.Constraints_Household2010 as Constraints_Household2010
import programs.constraints.Constraints_TenUnit2010 as Constraints_TenUnit2010
import programs.schema.table_building.building_DHCP_tables as Table_DHCP_HHGQ

###############################################################################
# Functions for Accessing Schemas, Invariants, and Constraints
###############################################################################
def getTableBuilder(schema_name: str) -> programs.schema.table_building.tablebuilder:
    """
    returns the TableBuilder object from the tablebuilder module associated with schema_name
    
    Inputs:
        schema_name (str): the name of the table builder schema class desired
    
    Outputs:
        a TableBuilder object
    """
    tablebuilder_modules = {
        con.DAS_PL94: Table_PL94,
        con.DAS_PL94_P12: Table_PL94_P12,
        con.DAS_PL94_CVAP: Table_PL94_CVAP,
        con.DAS_SF1: Table_SF1,
        con.DAS_Household2010: Table_Household2010,
        con.DAS_DHCP_HHGQ: Table_DHCP_HHGQ
    }
    assert schema_name in tablebuilder_modules, f"The '{schema_name}' tablebuilder module can't be found."
    tablebuilder = tablebuilder_modules[schema_name].getTableBuilder()
    return tablebuilder



def getSchema(schema_name: str) -> programs.schema.schema:
    """
    returns the Schema object from the schema module associated with schema_name
    
    Inputs:
        schema_name (str): the name of the schema class desired
    
    Outputs:
        a Schema object
    """
    schema_modules = {
        con.DAS_PL94: Schema_PL94,
        con.DAS_PL94_CVAP: Schema_PL94_CVAP,
        con.DAS_PL94_P12: Schema_PL94_P12,
        con.DAS_1940: Schema_1940,
        con.DAS_SF1: Schema_SF1,
        con.DAS_DHCP_HHGQ: Schema_DHCP_HHGQ,
        con.DAS_Household2010: Schema_Household2010,
        con.DAS_TenUnit2010: Schema_TenUnit2010,
        con.CC.SCHEMA_REDUCED_DHCP_HHGQ: schemamaker,
    }
    assert schema_name in schema_modules, f"The '{schema_name}' schema module can't be found."
    schema = schema_modules[schema_name].buildSchema()
    return schema


def getInvariantsModule(schema_name):
    """
    returns the invariants module associated with the schema_name
    
    Inputs:
        schema_name (str): the name of the schema class desired
    
    Outputs:
        an invariants module
    """
    invariant_modules = {
        con.DAS_PL94: Invariants_PL94,
        con.DAS_PL94_CVAP: Invariants_PL94_CVAP,
        con.DAS_PL94_P12: Invariants_PL94_P12,
        con.DAS_1940: Invariants_1940,
        con.DAS_SF1: Invariants_SF1,
        con.DAS_DHCP_HHGQ: Invariants_DHCP_HHGQ,
        con.DAS_Household2010: Invariants_Household2010,
        con.DAS_TenUnit2010: Invariants_TenUnit2010,
        con.CC.SCHEMA_REDUCED_DHCP_HHGQ: Invariants_DHCP_HHGQ,

    }
    assert schema_name in invariant_modules, f"The '{schema_name}' invariant module can't be found."
    return invariant_modules[schema_name]


def getConstraintsModule(schema_name):
    """
    returns the constraints module associated with the schema_name
    
    Inputs:
        schema_name (str): the name of the schema class desired
    
    Outputs:
        a constraints module
    """
    constraint_modules = {
        con.DAS_PL94: Constraints_PL94,
        con.DAS_PL94_CVAP: Constraints_PL94_CVAP,
        con.DAS_PL94_P12: Constraints_PL94_P12,
        con.DAS_1940: Constraints_1940,
        con.DAS_SF1: Constraints_SF1,
        con.DAS_DHCP_HHGQ: Constraints_DHCP_HHGQ,
        con.DAS_Household2010: Constraints_Household2010,
        con.DAS_TenUnit2010: Constraints_TenUnit2010,
        con.CC.SCHEMA_REDUCED_DHCP_HHGQ: Constraints_DHCP_HHGQ,
    }
    assert schema_name in constraint_modules, f"The '{schema_name}' constraint module can't be found."
    return constraint_modules[schema_name]
