with open("./input/titles-sorted.txt", "r", encoding="UTF-8") as title_file:
    titles = title_file.readlines()
with open("./input/links-simple-sorted.txt", "r") as links_file:
    links = links_file.readlines()
title_dict = {line: title for line, title in enumerate(titles)}
link_dict = {int(l.split(": ")[0]): l.split(": ")[1] for l in links}

# build the subgraph of "surfing" and add Rocky Mountain National Park
subgraph = {}
rocky = -1
for k, v in title_dict.items():
    if v == "Rocky_Mountain_National_Park\n":
        rocky = k
        subgraph[k] = list(map(int, link_dict[k].split(" ")))
    if "surfing" in v.lower():
        subgraph[k] = list(map(int, link_dict[k].split(" ")))

# add the links from Racky Mountain National Park to the subgraph
for k in list(map(int, link_dict[rocky].split(" "))):
    subgraph[k] = list(map(int, link_dict[k].split(" ")))

# bomb the subpraph and write it out
for key in subgraph:
    subgraph[key].append(rocky)
with open("./input/bombed_links.txt", "w") as f:
    for k, v in subgraph.items():
        outs = f"{k}: "
        for val in v:
            outs += f"{val} "
        outs = outs[:-1] + "\n"
        f.write(outs)

# with open("./input/bombed_links.txt", "w") as f:
#     for k, v in link_dict.items():
#         outs = f"{k}: "
#         if k in subgraph:
#             for val in subgraph[k]:
#                 outs += f"{val} "
#             outs = outs[:-1] + "\n"
#         else:
#             outs += v
#         f.write(outs)
