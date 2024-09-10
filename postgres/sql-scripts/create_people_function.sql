CREATE OR REPLACE FUNCTION read_people(delta_path text)
RETURNS TABLE(name text, age int, city text)
AS $$
from deltalake import DeltaTable

dt = DeltaTable(delta_path)
data = dt.to_pyarrow_table().to_pydict()
result = list(zip(data['Name'], data['Age'], data['City']))
return result
$$ LANGUAGE plpython3u;

