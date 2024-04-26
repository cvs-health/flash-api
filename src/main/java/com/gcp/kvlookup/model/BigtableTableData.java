/*
    Copyright 2024 CVS Health and/or one of its affiliates

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.gcp.kvlookup.model;

import javax.validation.constraints.NotBlank;
import java.util.List;

public class BigtableTableData {

    @NotBlank
    private String tableName;
    @NotBlank
    private String rowKeyId;
    @NotBlank
    private List<ColumnData> data;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKeyId() {
        return rowKeyId;
    }

    public void setRowKeyId(String rowKeyId) {
        this.rowKeyId = rowKeyId;
    }

    public List<ColumnData> getData() {
        return data;
    }

    public void setData(List<ColumnData> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "BigtableTableData{" +
                "tableName='" + tableName + '\'' +
                ", rowKeyId='" + rowKeyId + '\'' +
                ", data=" + data +
                '}';
    }
}
