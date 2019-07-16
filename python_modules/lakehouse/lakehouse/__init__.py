'''
A lakehouse is a dag of computation that defines the relationship
between computed tables in a data lake/data warehouse (get it?).

The user defines solids usings the @lakehouse_table definition.
These "tables" are used interchangeably as both types *and* solids.

The user defines functions with dagster input definitions with contain
those table types. One does this with lakehouse_table_input_def.

By having a 1:1 mapping between output type and solid lakehouse, and
by using these types as input types in dagster space, we can automatically
construct the dependency graph on the users behalf.

The user is also responsible for building a class that implements the
Lakehouse interface. This allows the user to totally customize where
these tables are actually materialized, and thus can be grafted on to
arbitrary data lake layouts.

See lakehouse_tests/__init__.py for a description of the use cases
implemented so far.
'''
from abc import ABCMeta, abstractmethod
from collections import defaultdict

import six

from dagster import (
    Any,
    DependencyDefinition,
    InputDefinition,
    Output,
    OutputDefinition,
    PipelineDefinition,
    SolidDefinition,
    check,
    define_python_dagster_type,
    execute_solid,
    resource,
)


class ITableHandle:
    pass


class TableHandle(ITableHandle):
    pass


class InMemTableHandle(ITableHandle):
    def __init__(self, value):
        self.value = value


def table_def_of_type(pipeline_def, type_name):
    for solid_def in pipeline_def.solid_defs:
        if (
            isinstance(solid_def, LakehouseTableDefinition)
            and solid_def.table_type.name == type_name
        ):
            return solid_def


class LakehouseTableDefinition(SolidDefinition):
    '''
    Trivial subclass, only useful for typehcecks and to implement table_type.
    '''

    def __init__(self, lakehouse_fn, output_defs, **kwargs):
        check.list_param(output_defs, 'output_defs', OutputDefinition)
        check.param_invariant(len(output_defs) == 1, 'output_defs')
        self.lakehouse_fn = lakehouse_fn
        super(LakehouseTableDefinition, self).__init__(output_defs=output_defs, **kwargs)

    @property
    def table_type(self):
        return self.output_defs[0].runtime_type


def _create_lakehouse_table_def(name, lakehouse_fn, input_defs, metadata=None, description=None):
    metadata = check.opt_dict_param(metadata, 'metadata')

    table_type = define_python_dagster_type(
        python_type=ITableHandle, name=name, description=description
    )

    table_type_inst = table_type.inst()

    input_def_dict = {input_def.name: input_def for input_def in input_defs}

    def _compute(context, inputs):
        '''
        Workhouse function of lakehouse. The inputs are something that inherits from ITableHandle.
        This compute_fn:
        (1) Iterates over input tables and ask the lakehouse resource to
         hydrate their contents or a representation of their contents
         (e.g a pyspark dataframe) into memory for computation
        (2) Pass those into the lakehouse table function. Do the actual thing.
        (3) Pass the output of the lakehouse function to the lakehouse materialize function.
        (4) Yield a materialization if the lakehouse function returned that.


        There's an argument that the hydrate and materialize functions should return
        a stream of events but that started to feel like I was implementing what should
        be a framework feature.
        '''

        # hydrate all things

        # TODO support non lakehouse table inputs
        hydrated_tables = {}
        for input_name, table_handle in inputs.items():
            context.log.info(
                'About to hydrate table {input_name} for use in {name}'.format(
                    input_name=input_name, name=name
                )
            )
            input_type = input_def_dict[input_name].runtime_type
            hydrated_tables[input_name] = context.resources.lakehouse.hydrate(
                context,
                input_type,
                table_def_of_type(context.pipeline_def, input_type.name).metadata,
                table_handle,
            )

        # call user-provided business logic which operates on the hydrated values
        # (as opposed to the handles)
        computed_output = lakehouse_fn(context, **hydrated_tables)

        materialization, output_table_handle = context.resources.lakehouse.materialize(
            context, table_type_inst, metadata, computed_output
        )

        if materialization:
            yield materialization

        # just pass in a dummy handle for now if the materialize function
        # does not return one
        yield Output(output_table_handle if output_table_handle else TableHandle())

    return LakehouseTableDefinition(
        lakehouse_fn=lakehouse_fn,
        name=name,
        input_defs=input_defs,
        output_defs=[OutputDefinition(table_type)],
        compute_fn=_compute,
        metadata=metadata,
        description=description,
    )


def lakehouse_table(name=None, input_defs=None, metadata=None, description=None):
    if callable(name):
        fn = name
        return _create_lakehouse_table_def(name=fn.__name__, lakehouse_fn=fn, input_defs=[])

    def _wrap(fn):
        return _create_lakehouse_table_def(
            name=name if name is not None else fn.__name__,
            lakehouse_fn=fn,
            input_defs=input_defs or [],
            metadata=metadata,
            description=description,
        )

    return _wrap


def lakehouse_table_input_def(name, lakehouse_table_def):
    check.str_param(name, 'name')
    check.inst_param(lakehouse_table_def, 'lakehouse_table_def', LakehouseTableDefinition)
    return InputDefinition(name, type(lakehouse_table_def.table_type))


def construct_lakehouse_pipeline(name, lakehouse_tables, mode_defs=None):
    '''
    Dynamically construct the pipeline from the table definitions
    '''
    solid_defs = lakehouse_tables
    type_to_solid = {}
    for solid_def in solid_defs:
        check.invariant(len(solid_def.output_defs) == 1)
        output_type_name = solid_def.output_defs[0].runtime_type.name
        check.invariant(output_type_name not in type_to_solid)
        type_to_solid[output_type_name] = solid_def

    dependencies = defaultdict(dict)

    for solid_def in solid_defs:
        for input_def in solid_def.input_defs:
            input_type_name = input_def.runtime_type.name
            check.invariant(input_type_name in type_to_solid)
            dependencies[solid_def.name][input_def.name] = DependencyDefinition(
                type_to_solid[input_type_name].name
            )

    return PipelineDefinition(
        name=name, mode_defs=mode_defs, solid_defs=solid_defs, dependencies=dependencies
    )


class Lakehouse(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    @abstractmethod
    def hydrate(self, context, table_type, table_metadata, table_handle):
        pass

    @abstractmethod
    def materialize(self, context, table_type, table_metadata, value):
        pass


class PySparkMemLakehouse(Lakehouse):
    def __init__(self):
        self.collected_tables = {}

    def hydrate(self, _context, _table_type, _table_metadata, table_handle):
        return table_handle.value

    def materialize(self, _context, table_type, _table_metadata, value):
        self.collected_tables[table_type.name] = value.collect()
        return None, InMemTableHandle(value=value)


@resource
def pyspark_mem_lakehouse_resource(_):
    return PySparkMemLakehouse()


def invoke_compute(table_def, inputs, mode_def=None):
    '''
    Invoke the core computation defined on a table directly.
    '''

    def _compute_fn(context, _):
        yield Output(table_def.lakehouse_fn(context, **inputs))

    return execute_solid(
        SolidDefinition(
            name='wrap_lakehouse_fn_solid',
            input_defs=[],
            output_defs=[OutputDefinition(Any)],
            compute_fn=_compute_fn,
        ),
        mode_def=mode_def,
    )
