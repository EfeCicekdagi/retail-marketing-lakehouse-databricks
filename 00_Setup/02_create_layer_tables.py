{
  "datasets": [
    {
      "name": "8e97bdf5",
      "displayName": "vw_daily_sales_kpi",
      "config": {
        "version": "1.1",
        "source": "retail_marketing.dm_marketing.vw_daily_sales_kpi",
        "dimensions": [
          {
            "expr": "source.*"
          }
        ],
        "measures": [
          {
            "name": "count",
            "expr": "COUNT(*)",
            "comment": "Represents the total number of rows in the dataset. Use this measure to count all",
            "displayName": "Count"
          }
        ]
      }
    },
    {
      "name": "e4413fb4",
      "displayName": "daily_sales_kpi",
      "config": {
        "version": "1.1",
        "source": "SELECT\r\n    DayNumber,\r\n    WeekNumber,\r\n    BasketCount,\r\n    HouseholdCount,\r\n    ProductCount,\r\n    StoreCount,\r\n    TotalQuantity,\r\n    GrossSalesAmount,\r\n    TotalDiscountAmount,\r\n    NetSalesAmount,\r\n    CouponTransactionCount,\r\n    AverageBasketAmount,\r\n    DiscountRate,\r\n    CouponTransactionRate,\r\n    SourceBatchID,\r\n    ProcessDate\r\nFROM retail_marketing.dm_marketing.vw_daily_sales_kpi",
        "dimensions": [
          {
            "expr": "source.*"
          }
        ],
        "measures": [
          {
            "name": "count",
            "expr": "COUNT(*)",
            "comment": "Represents the total number of rows in the dataset. Use this measure to count all",
            "displayName": "Count"
          }
        ]
      }
    },
    {
      "name": "6ab3bfc0",
      "displayName": "vw_daily_sales_kpi",
      "config": {
        "version": "1.1",
        "source": "retail_marketing.dm_marketing.vw_daily_sales_kpi",
        "dimensions": [
          {
            "expr": "source.*"
          }
        ],
        "measures": [
          {
            "name": "count",
            "expr": "COUNT(*)",
            "comment": "Represents the total number of rows in the dataset. Use this measure to count all",
            "displayName": "Count"
          }
        ]
      }
    },
    {
      "name": "6a2200f9",
      "displayName": "Top Retail Product Sales Performance Summary",
      "queryLines": [
        "SELECT\r\n",
        "    ProductID,\r\n",
        "    Department,\r\n",
        "    Brand,\r\n",
        "    CommodityDescription,\r\n",
        "    BasketCount,\r\n",
        "    HouseholdCount,\r\n",
        "    StoreCount,\r\n",
        "    TotalQuantity,\r\n",
        "    GrossSalesAmount,\r\n",
        "    TotalDiscountAmount,\r\n",
        "    NetSalesAmount,\r\n",
        "    CouponTransactionCount,\r\n",
        "    AverageUnitNetSales,\r\n",
        "    DiscountRate,\r\n",
        "    CouponUsageRate,\r\n",
        "    NetSalesRank,\r\n",
        "    QuantityRank,\r\n",
        "    SourceBatchID,\r\n",
        "    ProcessDate\r\n",
        "FROM retail_marketing.dm_marketing.vw_top_products"
      ]
    }
  ],
  "pages": [
    {
      "name": "ffbf1176",
      "displayName": "Untitled page",
      "layout": [
        {
          "widget": {
            "name": "net-sales",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "sum(NetSalesAmount)",
                      "expression": "SUM(`NetSalesAmount`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Net Sales"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "sum(NetSalesAmount)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "gross-sales",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "sum(GrossSalesAmount)",
                      "expression": "SUM(`GrossSalesAmount`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Gross Sales"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "sum(GrossSalesAmount)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 2,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "basket-count",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "sum(BasketCount)",
                      "expression": "SUM(`BasketCount`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Basket Count"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "sum(BasketCount)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 4,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "avg-basket-amount",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "avg(AverageBasketAmount)",
                      "expression": "AVG(`AverageBasketAmount`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Average Basket Amount"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "avg(AverageBasketAmount)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "discount-rate",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "avg(DiscountRate)",
                      "expression": "AVG(`DiscountRate`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Discount Rate"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "avg(DiscountRate)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 8,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "coupon-rate",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "avg(CouponTransactionRate)",
                      "expression": "AVG(`CouponTransactionRate`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Coupon Usage Rate"
              },
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "avg(CouponTransactionRate)"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 10,
            "y": 0,
            "width": 2,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "daily-net-sales-trend",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "DayNumber",
                      "expression": "`DayNumber`"
                    },
                    {
                      "name": "NetSalesAmount",
                      "expression": "`NetSalesAmount`"
                    },
                    {
                      "name": "GrossSalesAmount",
                      "expression": "`GrossSalesAmount`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "version": 3,
              "frame": {
                "showTitle": true,
                "title": "Daily Net Sales Trend"
              },
              "widgetType": "line",
              "encodings": {
                "x": {
                  "fieldName": "DayNumber",
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "y": {
                  "scale": {
                    "type": "quantitative"
                  },
                  "fields": [
                    {
                      "fieldName": "NetSalesAmount"
                    },
                    {
                      "fieldName": "GrossSalesAmount"
                    }
                  ]
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 3,
            "width": 6,
            "height": 6
          }
        },
        {
          "widget": {
            "name": "gross-sales-discounts-net-sales",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "8e97bdf5",
                  "fields": [
                    {
                      "name": "DayNumber",
                      "expression": "`DayNumber`"
                    },
                    {
                      "name": "GrossSalesAmount",
                      "expression": "`GrossSalesAmount`"
                    },
                    {
                      "name": "TotalDiscountAmount",
                      "expression": "`TotalDiscountAmount`"
                    },
                    {
                      "name": "NetSalesAmount",
                      "expression": "`NetSalesAmount`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "version": 3,
              "frame": {
                "showTitle": true,
                "title": "Gross Sales, Discounts and Net Sales"
              },
              "widgetType": "scatter",
              "encodings": {
                "x": {
                  "fieldName": "DayNumber",
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "y": {
                  "scale": {
                    "type": "quantitative"
                  },
                  "fields": [
                    {
                      "fieldName": "GrossSalesAmount"
                    },
                    {
                      "fieldName": "TotalDiscountAmount"
                    },
                    {
                      "fieldName": "NetSalesAmount"
                    }
                  ]
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 3,
            "width": 6,
            "height": 6
          }
        },
        {
          "widget": {
            "name": "0e411025",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "6a2200f9",
                  "fields": [
                    {
                      "name": "sum(ProductID)",
                      "expression": "SUM(`ProductID`)"
                    },
                    {
                      "name": "NetSalesAmount",
                      "expression": "`NetSalesAmount`"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 3,
              "widgetType": "bar",
              "encodings": {
                "x": {
                  "fieldName": "sum(ProductID)",
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "y": {
                  "fieldName": "NetSalesAmount",
                  "scale": {
                    "type": "quantitative"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 9,
            "width": 6,
            "height": 6
          }
        }
      ],
      "pageType": "PAGE_TYPE_CANVAS",
      "layoutVersion": "GRID_V1"
    }
  ],
  "uiSettings": {
    "theme": {
      "widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"
    },
    "applyModeEnabled": false
  }
}
