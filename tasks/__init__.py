"""Package containing all the tasks to be performed within the automdeling pipeline.

Each module corresponds to an airflow task to be performed. Within each module at least
2 functions are defined: 
    * one function with the same name as the module corresponds to the pure logic of the
        ask (not accomodating for file resolution, s3, airflow ...). 
    * The second required function share the same name as the module suffixed with
        `_task`. This function must be decorated using the
        :func:`automodeling.utils.airflow.airflow_task` decorator producing a valid
        airflow task for the DAG.

Following this convention helps automatically resolving DAG from the package.

It contains the following tasks:
    * :mod:`pdf_to_md`
    * :mod:`md_to_scenarios`
    * :mod:`md_to_ontology`
    * :mod:`all_to_graph`
"""
