'''
Main file that establishes connection to a PostGreSQL database and launches an interactive web-based GUI.
'''

import gradio as gr
from interface import PGInterface


def launch_interface():
    print(gr.__version__)
    with gr.Blocks() as gui:
        gr.Markdown('# Postgres QEP GUI done on Gradio by Group 13')
        gr.Markdown('Connect to a PostgreSQL database on pgAdmin4, execute a Query, and view QEP Visualization.')
        PGInterface().createInterface()

    gui.launch(inbrowser=True)


if __name__ == '__main__':
    launch_interface()
