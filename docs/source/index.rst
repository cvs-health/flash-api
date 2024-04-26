Quickstart
----------
The project is intended to provide out of the box APIs to various GCP data stores for CRUD operations. Its purpose is to
provide in-built solution to integrate with any GCP data store by just adding required configuration. This API can be integrated
in current systems with very minimal efforts. Currently It supports GCP BigTable data store and we are working on adding support to other GCP datastores.

GCP KV REST API is a Springboot application for various GCP data stores.
Its purpose is to provide in-built solution to integrate with any GCP data store by just adding required configuration. This API can be integrated
in current systems with very minimal efforts. Currently It supports GCP BigTable data store and we are working on adding support to other GCP datastores.

We see that many times we require APIs which are mostly fetching/updating the some information using a specific id and this is mostly recurring requirement
with changes in data store. It can be BIG table, redis or cloud SQL.


This application provides out of the box solutions for all these kind of requirements.
All you need to do is just add configuration for specific data stores and you are ready to use this in your application!

.. container::

Installation
------------

This application needs to have access to enabled GCP services from your local machine. You can install gcloud sdk and run below command to authenticate and setup gcp access

.. code:: bash

   gcloud auth application-default login

Update application.properties for below variables for you GCP project or setup env variables as below

.. code:: bash

       gcp.projectId=${GCP_PROJECT_ID}
       gcp.instanceId.list=${GCP_BIGTABLE_INSTANCE_LIST_COMMA_SEPARATED}

You need to have Java 11 with maven installed and get required dependencies via:

.. code:: bash

        mvn clean package

Run KVLookupApplication.java from ide or using below command from terminal

.. code:: bash

         java -Dlog4j2.formatMsgNoLookups=true -DGCP_PROJECT_ID=<gcp_project_id> -DGCP_BIGTABLE_INSTANCE_LIST=<comma-seprated-list-of-instanceIds> -DGOOGLE_APPLICATION_CREDENTIALS=<path-to/application_default_credentials.json> -jar target/gcp-kv-crud-rest-api-0.0.1-SNAPSHOT.jar

Once started access API using:

    http://localhost:8080/swagger-ui/index.html

Usage
-----

The code currently provides below APIs

- POST /v1/{instanceID}/insertData

This POST API will take instanceId as a path parameter and request payload as below which consists of tableName, rowKeyId and data.

.. code-block:: json

    {
      "tableName": "<Name of the table>",
      "rowKeyId": "<Id of the row>",
      "data": [
        {
          "columnFamily": "string",
          "columnName": "string",
          "columnValue": "string"
        }
      ]
    }

- POST /v1/{instanceID}/createTable

This POST API will create a new table in given BigTable instance along with a columnFamily mentioned.

.. code-block:: json

    {
      "tableName": "string",
      "columnFamily": "string"
    }

- GET /v1/{instanceID}/readCellData

This GET API takes path parameter instandID and query parameters tableName, id. It returns all values of given row as a response.

- DELETE /v1/{instanceID}/deleteTable

This DELETE API will delete a tables that are listed in request payload in given instanceId

.. code-block:: json

    {
      "tableList": [
        "string"
      ]
    }

Features Under Development
--------------------------

-  simplify parameter names to accomodate multiple datastores
-  Add new parameter to identify specific datastore
-  Add support for Spanner and Redis

Contributor Guide
-----------------

1. Before contributing to this CVS Health sponsored project, you will
   need to sign the associated `Contributor License
   Agreement <https://forms.office.com/r/EyZTFf6tjm>`__.
2. See `contributing <CONTRIBUTING.md>`__ page.
