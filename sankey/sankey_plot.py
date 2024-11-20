import plotly.graph_objects as go
from random import randint

# Define the taxonomy mapping and colors
index2taxonomies = {
    0: "Syntax Error",
    1: "Schema Error",
    2: "Logic Error",
    3: "Convention Error",
    4: "Semantic Error",
    5: "Not an Error",
    6: "Others",
    7: "Correct",
}

# Define colors for nodes (based on the taxonomies' order)
node_colors = [
    "#ff6961", "#ffb480", "#f8f38d", "#42d6a4", 
    "#08cad1", "#59adf6", "#9d94ff", "#c780e8"
]
node_colors = [
    "#ed9392", "#eecb8e", "#e0e181", "#93eaa8", 
    "#85cfcd", "#9ccde3", "#dfaeda", "#b2b2b2"
]
node_colors = [
    "#ee8d8b", "#eecc89", "#e1e175", "#88eba3", 
    "#7bcfce", "#8ecce5", "#dfa6db", "#b0b0b0"
]

light_grey = "#d3d3d3"  # Light grey color for non-highlighted nodes

node_colors = node_colors * 2  # Duplicate the colors for source and target nodes

# Set the source and target based on `index2taxonomies` order
source = [x for x in range(8) for _ in range(8)]  # Source: ordered as per the taxonomy
target = [y + 8 for _ in range(8) for y in range(8)]  # Target: same order as source, starting from 8 onwards
# value = [randint(1, 10) for _ in range(64)]  # Example values for links

import json
with open('sankey_metadata.json', 'r') as f:
    value = json.load(f)

def get_link_color(source_index, target_index, highlight=False):
    """
    Get a lighter version of the target node's color by applying transparency.
    """
    base_color = node_colors[target_index] if highlight else light_grey  # Use the target node's color for highlighted links
    transparency = 0.7 if highlight else 0.2  # More transparency for non-highlighted parts
    return f"rgba({int(base_color[1:3], 16)}, {int(base_color[3:5], 16)}, {int(base_color[5:], 16)}, {transparency})"  # Add transparency

def get_line_style(index, highlight=False):
    """
    Get line style based on the target node's color.
    """
    if highlight or index >= 8:
        return dict(color="black", width=1)  # Hide the line for highlighted nodes
    else:
        return dict(color="black", width=0)  # Show the line for non-highlighted nodes

def plot_highlighted_graph(highlight_index):
    # Create node labels with the same order and colors as `index2taxonomies`
    labels = ["" for x in range(8)] + ["" for x in range(8)]
    colors = []  # Node colors with transparency based on the highlight_index
    lines = [color for color in [get_line_style(i, highlight=(i == highlight_index)) for i in range(16)]]
    from pprint import pprint
    # pprint(source)
    # pprint(target)
    
    

    # Calculate total value for each targeting node
    print(value)
    total_value = [sum(value[i:64:8]) for i in range(8)]
    total_value_ = [sum(value[i:i+8]) for i in range(0, 64, 8)]
    # Calculate the percentage of flowin of target node
    percentages = [f"{(value[highlight_index * 8 + i] / total_value[i]) * 100:.1f}%" for i in range(8)]
    percentages_ = [f"{(value[highlight_index * 8 + i] / total_value_[highlight_index]) * 100:.1f}%" for i in range(8)]
    
    for i in range(8):
        print(value[highlight_index * 8 + i], total_value_[highlight_index], percentages_[i])
    

    for i in range(16):
        if i == highlight_index or i >= 8:
            colors.append(node_colors[i]) 
        else:
            colors.append(f"rgba({int(node_colors[i][1:3], 16)}, {int(node_colors[i][3:5], 16)}, {int(node_colors[i][5:], 16)}, 0.4)")  # Highly transparent color


    # Define link colors based on target node colors with transparency
    link_colors = [get_link_color(s, t - 8, highlight=(s == highlight_index)) for s, t in zip(source, target)]

    # Add percentage to labels for the highlighted node
    # labels[highlight_index] += f" ({percentages[highlight_index]})"
    for i in range(8):
        labels[i + 8] += f"{percentages_[i]}"

    # node x cordination
    x = [0.25] * 8 + [0.75] * 8
    # calc each class num for source and target
    source_num = [sum(value[i:i+8]) for i in range(0, 64, 8)]
    target_num = [sum(value[i:64:8]) for i in range(8)]
    
    # make space of node in y axis evenly
    d = 0.02
    space_portion = (1 / 4) / (8 - 1)
    print(space_portion)
    y = [0.0001 + i * space_portion +  (sum(source_num[:i]) + 0.5 * source_num[i]) / (sum(source_num) * 1.25) for i in range(8)] + [0.0001 + i * space_portion + (sum(target_num[:i]) + 0.5 * target_num[i]) / (sum(target_num) * 1.25)  for i in range(8)]
        

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=25,  # Padding between nodes
            thickness=60,  # Thickness of nodes
            line=dict(width=0),  # Set line width to 0.5
            label=labels,  # Set the node labels with percentage
            color=colors,  # Set the node colors according to the taxonomy order and transparency
            x=x,
            y=y
        ),
        link=dict(
            source=source,  # Keep source in predefined order
            target=target,  # Keep target in predefined order
            value=value,  # Use link values for flow size
            color=link_colors,  # Set link colors based on the target node colors with transparency
            line=dict(width=0),  # Set line width to 0.5
            hoverinfo="all"  # Display detailed info on hover
        )
    )])
    lab = 'a' if highlight_index == 0 else 'b' if highlight_index == 1 else 'c' if highlight_index == 2 else 'd' if highlight_index == 3 else 'e' if highlight_index == 4 else 'f' if highlight_index == 5 else 'g' if highlight_index == 6 else 'h' if highlight_index == 7 else 'i' 
    
    # fig.update_layout(title_text=f'({lab}) {index2taxonomies[highlight_index]}', title_x=0.5, title_y=0.02)
    # Update layout for better aesthetics and set font to Cambria
    fig.update_layout(
        # title_text="Sankey Diagram with Highlight and Percentage Labels",
        font=dict(family='Charter-Bold', size=50, color='black'),
        title_font_size=48,
        title_font_family="Times New Roman",
        margin=dict(l=10, r=10, t=40, b=40),  # Adjust margins for better spacing
        plot_bgcolor='rgba(0,0,0,0)',  # Set plot background color to transparent
    )
    fig.update_layout(font_family="Cambria-Bold")
    fig.update_layout(
        {
            "paper_bgcolor": "rgba(0, 0, 0, 0)",
            "plot_bgcolor": "rgba(0, 0, 0, 0)",
        }
    )
    plotly.io.full_figure_for_development(fig, warn=False)
    # fig.show()
    fig.write_image(f"images/{index2taxonomies[highlight_index].split(' ')[0]}.pdf", width=1000, height=900, format='pdf', engine="kaleido")
    import time
    time.sleep(2)
    fig.write_image(f"images/{index2taxonomies[highlight_index].split(' ')[0]}.pdf", width=1000, height=900, format='pdf', engine="kaleido")
        
if __name__ == '__main__':
    highlight_index = 0  # Set the index ofthe node to highlight (e.g., 2 for "Logic Error")
    import os
    import plotly.io
    
    # plotly.io.kaleido.scope.default_width = 1920
    # plotly.io.kaleido.scope.default_height = 1920

    for i in range(8):
        plot_highlighted_graph(i)
    
    # print(plotly.io.kaleido.scope.default_width)
    # plotly.io.kaleido.scope.default_width = 800