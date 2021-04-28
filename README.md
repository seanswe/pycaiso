## caisotools AKA pycaiso

pycaiso is a python wrapper for the California Independent System Operator (CAISO) Open Access Same-time Information System (OASIS) API. 

pycaiso currently supports the following reports and resultsets:

1. Prices
2. System Demand
3. Atlas

## Examples

Get locational marginal prices (LMPs) for arbitrary Node and period:

```
from oasis.oasis import Node
from datetime import datetime

# select pnode
cj = Node("CAPTJACK_5_N003")

# create dataframe with LMPS from arbitrary period (30 day maximum)
cj_lmps = cj.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))

print(cj_lmps.head())

```

Alternatively, you can use pre-built Nodes for major aggregated pricing nodes (apnodes) like SP15:

```
# use pre-built pnode
sp15 = Node.SP15()

sp15_lmps = sp15.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))

print(sp15.head())

```


