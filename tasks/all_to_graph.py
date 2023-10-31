"""Module for the `all_to_graph` task using previous task output to generate a graph.

This task leverage scenarios and ontology generated during previous tasks to automodel
a computable contract graph representation.

it contains:
    * :func:`all_to_graph`: actual logic taking an ontology str and scenarios str and
        producing a graph.
    * :func:`all_to_graph_task`: a decorated airflow task wrapping the logic to be used
        in the DAG.

"""
import os

from utils.airflow import airflow_task


def all_to_graph(ontology: str, scenarios: str):
    """Creates a json graph from ontology and scenarios.

    Args:
        ontology: a string representation of the ontology (json formatted).
        scenarios: a string representation of scenarios extracted from MD.

    Returns:
        a str formatted as json corresponding to a computable contract graph.
    """
    return "{'node': 'coverage'}"


@airflow_task(
    s3folder_inputs=["s3://md_to_ontology/", "s3://md_to_scenarios/"],
    s3folder_outputs=["s3://all_to_graph/"],
)
def all_to_graph_task(
    ontology_local_path, scenarios_local_path
):
    """Airflow task wrapping :func:`all_to_graph`.

    Notes:
        * s3folder_inputs lenght must match with the function args length.
        * s3folder_outputs lenght must match with the function output length.

    Args:
        ontology_local_path: a path to the locally downloaded ontology file resolved
            from the corresponding s3folder_input and airflow io.
        scenarios_local_path: a path to the locally downloaded scenarios file resolved
            from the corresponding s3folder_input and airflow io.

    Returns:

        a list of tuple containing 2 str values for each output of :func:`all_to_graph`:
            * filename: the filename desired for the given output including extension.
            * content: the str content that will be written to the file.

    """
    # the decorator :func:`airflow_task` resolve s3 folder path into local file only
    # any further processing of the files must be defined here to accomodate
    # :func:`all_to_graph` inputs.
    """
    with open(ontology_local_path, "r") as f:
        json_content: str = f.read()

    with open(scenarios_local_path, "r") as f:
        txt_content: str = f.read()
    """
    # call the pure logic function defined above.
    # ----------modified for test------------#
    #graph_content = all_to_graph(json_content, txt_content)
    graph_content="graph_content"
    # create the list of tuple for the decorator to correctly write back the file and
    # share the depedency with the next step.
    filename = os.path.basename(ontology_local_path).split(".")[0] + "_graph.json"
    return [(filename, graph_content)]
