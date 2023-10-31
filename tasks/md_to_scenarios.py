"""Module for the `md_to_scenarios` task using MD representation to create scenarios.

This task leverage a previously generated MD representation of a contract to generate
scenarios as a txt file.

it contains:
    * :func:`md_to_scenarios`: actual logic taking an MD str and producing scenarios as
        str.
    * :func:`md_to_scenarios_task`: a decorated airflow task wrapping the logic to be
        used in the DAG.

"""
import os

from utils.airflow import airflow_task


def md_to_scenarios(md):
    """Creates scenarios from MD content.

    Args:
        md: a str representing a markdown file.

    Returns:
        a list of scenarios str.
    """
    return [
        "scenario 1 - content 1",
        "scenario 2 - content 2",
        "scneario 3 - content 3",
    ]


@airflow_task(
    s3folder_inputs=["s3://pdf_to_md/"], s3folder_outputs=["s3://md_to_scenarios/"]
)
def md_to_scenarios_task(md_local_path):
    """Airflow task wrapping :func:`md_to_scenarios`.

    Notes:
        * s3folder_inputs lenght must match with the function args length.
        * s3folder_outputs lenght must match with the function output length.

    Args:
        md_local_path: a path to the locally downloaded MD file resolved from the
            corresponding s3folder_input and airflow io.

    Returns:

        a list of tuple containing 2 str values for each output of :func:`md_to_scenarios`:
            * filename: the filename desired for the given output including extension.
            * content: the str content that will be written to the file.

    """
    # the decorator :func:`airflow_task` resolve s3 folder path into local file only
    # any further processing of the files must be defined here to accomodate
    # :func:`md_to_scenarios` inputs.
    """
    with open(md_local_path, "r") as f:
        md_content: str = f.read()
    """
    # call the pure logic function defined above.
    """
    scenarios_content = md_to_scenarios(md_content)
    """
    # create the list of tuple for the decorator to correctly write back the file and
    # share the depedency with the next step. Because :func:`md_to_scenarios` returns
    # a list of str we need to first join them into a single str.

    # --------Modified for test---------#
    #scenarios_content_str = "\n".join(scenarios_content)
    scenarios_content_str = "\n".join("senarios_content")

    # define output filename based on pdf name
    filename = os.path.basename(md_local_path).split(".")[0] + ".txt"

    return [(filename, scenarios_content_str)]
