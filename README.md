## pycaiso

pycaiso is a python wrapper for the California Independent System Operator (CAISO) Open Access Same-time Information System (OASIS) API. 

pycaiso currently supports the following reports and resultsets:

1. Prices
2. System Demand
3. Atlas

## Examples

Get locational marginal prices (LMPs) in Day Ahead Market (DAM) for arbitrary Node and period:
Note: "DAM" is the default market. You can also set the market parameter in get_lmps to "RTM" or "RTPD". 

```
from pycaiso.oasis import Node
from datetime import datetime
import pandas as pd

# select pnode

cj = Node("CAPTJACK_5_N003")

# create dataframe with LMPS from arbitrary period (30 day maximum). 

cj_lmps = cj.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))

print(cj_lmps.head())

#        INTERVALSTARTTIME_GMT        INTERVALENDTIME_GMT  ...        MW  GROUP
# 0  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  33.32310      1
# 1  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...   0.00000      2
# 2  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  34.68627      3
# 3  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  -1.36317      4
# 4  2021-01-01T09:00:00-00:00  2021-01-01T10:00:00-00:00  ...  31.51635      1
```

Alternatively, you can use pre-built Nodes for major aggregated pricing nodes (apnodes) like SP15:

```
# use pre-built pnode
sp15 = Node.SP15()

sp15_lmps = sp15.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))

print(sp15_lmps.head())

#        INTERVALSTARTTIME_GMT        INTERVALENDTIME_GMT  ...        MW  GROUP
# 0  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  33.48613      1
# 1  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...   0.00000      2
# 2  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  34.68627      3
# 3  2021-01-01T08:00:00-00:00  2021-01-01T09:00:00-00:00  ...  -1.20014      4
# 4  2021-01-01T09:00:00-00:00  2021-01-01T10:00:00-00:00  ...  31.58175      1
```


