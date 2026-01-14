### IPDB Server API
This project simulates the [IPinfo](https://ipinfo.io/) API using the public [Kaggle database](https://www.kaggle.com/datasets/ipinfo/ipinfo-country-asn).

- The goal is to provide fast access to IP data (IPv4 or IPv6) through an API.
- The API is simple: send a [POST] request to "/" with a JSON body containing the `ip` field. The server handles errors for incorrectly formatted requests.
- Data is loaded into RAM to support high request volumes, offering better performance than raw MongoDB or indexed MongoDB (which can be slow due to I/O).
- RAM usage can be optimized by preprocessing the `.csv` file with Python and loading it as JSONL, or by using a database directly. But I really wanted the other data stadistics.

###### 🖤