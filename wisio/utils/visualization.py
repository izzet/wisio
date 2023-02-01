import graphviz
import networkx as nx
from dask.distributed import Future

def save_graph(future: Future, filename: str):
    dask_graph = future.__dask_graph__()
    edges = []
    edge_count = {}
    for key, value in dask_graph.dependencies.items():
        vals = list(value)
        destination = key.split("-")[0]
        for val in vals:
            source = val.split("-")[0]
            str_edge = str(source) + "-" + str(destination)
            if str_edge not in edge_count:
                edge_count[str_edge] = 0
            edge_count[str_edge] = edge_count[str_edge] + 1
            edges.append((source, destination))
    dag = nx.DiGraph()
    dag.add_edges_from(edges)
    dot = graphviz.Digraph()
    for node in dag.nodes:
        dot.node(node)
    for edge_str, count in edge_count.items():
        source, destination  = edge_str.split("-")
        dot.edge(source, destination, label=str(count))
    dot.render(filename=filename, format='png', view=False)
    # return dot
