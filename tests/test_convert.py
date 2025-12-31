# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests conversion functions."""

from __future__ import annotations

import json
import unittest

from pydgraph import convert


class TestConvert(unittest.TestCase):
    """Tests conversion functions."""

    def test_extract_nodes_edges(self) -> None:
        # no ids, no extraction
        nodes: dict[str, dict[str, object]] = {}
        edges: list[dict[str, object]] = []
        convert.extract_dict(
            nodes=nodes, edges=edges, data=json.loads(sample_json_empty_result)
        )
        assert len(nodes) == 0
        assert len(edges) == 0

        # graphQL result extraction
        nodes = {}
        edges = []
        convert.extract_dict(
            nodes=nodes, edges=edges, data=json.loads(sample_json_graphql_result)
        )
        assert len(nodes) == 3
        assert len(edges) == 3
        assert nodes["100600993"]["countries"] == ["USA", "UK"]
        assert nodes["100600993"]["foo"] == "bar"

        # complex extraction with replicated entities
        nodes = {}
        edges = []
        convert.extract_dict(
            nodes=nodes, edges=edges, data=json.loads(sample_json_dql_result)
        )
        assert len(nodes) == 61
        assert len(edges) == 109
        donation = nodes["0x156900"]["amount"]
        assert donation == 100
        edge = edges[0]
        assert edge["src"] == "0x4b"
        assert edge["type"] == "donations"


def suite() -> unittest.TestSuite:
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestConvert())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())

# Test Fixtures
sample_json_empty_result = """
{
    "foo": "bar",
    "children": [
        {
            "bar": "foo",
            "type": "Child"
        }
    ]
}
"""

sample_json_graphql_result = """
{
  "data": {
    "queryEntity": [
      {
        "id": "100600993",
        "type": "Entity",
        "name": "MANOR INVESTMENTS LIMITED",
        "foo": "bar",
        "countries": ["USA", "UK"],
        "hasAddress": [
          {
            "id": "120017700",
            "type": "Address",
            "name": "CHANCERY CHAMBERS, CHANCERY HOUSE , HIGH STREET, BRIDGETOWN, BARBADOS.",
            "addressFor": [
              {
                "id": "100600993",
                "type": "Entity",
                "name": "MANOR INVESTMENTS LIMITED"
              },
              {
                "id": "100613826",
                "type": "Entity",
                "name": "CRESTOVE PROPERTIES LIMITED"
              }
            ]
          }
        ]
      }
    ]
  }
}
"""

sample_json_dql_result = """
{
  "data": {
    "q": [
      {
        "id": "0x4b",
        "title": "Current Events in Second Grade",
        "type": [
          "Project"
        ],
        "donations": [
          {
            "id": "0x3164e",
            "type": [
              "Donation"
            ],
            "amount": 75,
            "project": {
              "id": "0x4b"
            }
          },
          {
            "id": "0x7c3b3",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x4b"
            }
          },
          {
            "id": "0x13d9aa",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x4b"
            }
          },
          {
            "id": "0x16e3a5",
            "type": [
              "Donation"
            ],
            "amount": 19.82,
            "project": {
              "id": "0x4b"
            }
          },
          {
            "id": "0x219b30",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x4b"
            }
          },
          {
            "id": "0x2f69a3",
            "type": [
              "Donation"
            ],
            "amount": 10,
            "project": {
              "id": "0x4b"
            }
          }
        ],
        "category": {
          "id": "0x1283b4",
          "type": [
            "Category"
          ],
          "name": "History & Civics, Literacy & Language"
        }
      },
      {
        "id": "0x58",
        "title": "Great Green Garden Gables",
        "type": [
          "Project"
        ],
        "donations": [
          {
            "id": "0xdbd4e",
            "type": [
              "Donation"
            ],
            "amount": 1,
            "project": {
              "id": "0x58"
            }
          },
          {
            "id": "0x156900",
            "type": [
              "Donation"
            ],
            "amount": 100,
            "project": {
              "id": "0x58"
            }
          },
          {
            "id": "0x1a1f25",
            "type": [
              "Donation"
            ],
            "amount": 15,
            "project": {
              "id": "0x58"
            }
          },
          {
            "id": "0x1d29a5",
            "type": [
              "Donation"
            ],
            "amount": 10,
            "project": {
              "id": "0x58"
            }
          },
          {
            "id": "0x1d2a86",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x58"
            }
          },
          {
            "id": "0x2c4078",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x58"
            }
          }
        ],
        "category": {
          "id": "0x203ddc",
          "type": [
            "Category"
          ],
          "name": "Applied Learning, Math & Science"
        }
      },
      {
        "id": "0x6a",
        "title": "Albert.io Prepares South LA students for AP Success!",
        "type": [
          "Project"
        ],
        "donations": [
          {
            "id": "0x3057",
            "type": [
              "Donation"
            ],
            "amount": 163.09,
            "project": {
              "id": "0x6a"
            }
          },
          {
            "id": "0xc3569",
            "type": [
              "Donation"
            ],
            "amount": 100,
            "project": {
              "id": "0x6a"
            }
          },
          {
            "id": "0x10d735",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x6a"
            }
          },
          {
            "id": "0x1d2415",
            "type": [
              "Donation"
            ],
            "amount": 20,
            "project": {
              "id": "0x6a"
            }
          },
          {
            "id": "0x219865",
            "type": [
              "Donation"
            ],
            "amount": 10,
            "project": {
              "id": "0x6a"
            }
          }
        ],
        "category": {
          "id": "0x189cb1",
          "type": [
            "Category"
          ],
          "name": "Applied Learning, Literacy & Language"
        }
      },
      {
        "id": "0x76",
        "title": "Learning and Growing Through Collaborative Play in TK!",
        "type": [
          "Project"
        ],
        "donations": [
          {
            "id": "0x30db2",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x498b9",
            "type": [
              "Donation"
            ],
            "amount": 100,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x7b35e",
            "type": [
              "Donation"
            ],
            "amount": 30,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x7b5b2",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0xabfff",
            "type": [
              "Donation"
            ],
            "amount": 100,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x16f469",
            "type": [
              "Donation"
            ],
            "amount": 15,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x16f599",
            "type": [
              "Donation"
            ],
            "amount": 1,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x1a041b",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x1a0bd5",
            "type": [
              "Donation"
            ],
            "amount": 200.87,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x1d21a2",
            "type": [
              "Donation"
            ],
            "amount": 15,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x202ff5",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x24bb83",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x27ae47",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x27ae58",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x295cad",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x2ad96a",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x2c626e",
            "type": [
              "Donation"
            ],
            "amount": 100,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x2dc971",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x76"
            }
          },
          {
            "id": "0x2dd1ff",
            "type": [
              "Donation"
            ],
            "amount": 40,
            "project": {
              "id": "0x76"
            }
          }
        ],
        "category": {
          "id": "0x1408c2",
          "type": [
            "Category"
          ],
          "name": "Literacy & Language, Math & Science"
        }
      },
      {
        "id": "0x82",
        "title": "Sit Together, Learn Together, Grow Together!",
        "type": [
          "Project"
        ],
        "donations": [
          {
            "id": "0x18727",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x7c01a",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x7c43c",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0xc35e2",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0xdcfc3",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x10d857",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x1e963e",
            "type": [
              "Donation"
            ],
            "amount": 20,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x201a3b",
            "type": [
              "Donation"
            ],
            "amount": 20,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x2324f4",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x24ac0e",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x27b032",
            "type": [
              "Donation"
            ],
            "amount": 25,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x27b5c0",
            "type": [
              "Donation"
            ],
            "amount": 50,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x295a4b",
            "type": [
              "Donation"
            ],
            "amount": 10,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x2c4b3f",
            "type": [
              "Donation"
            ],
            "amount": 20,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x2c52ec",
            "type": [
              "Donation"
            ],
            "amount": 28.84,
            "project": {
              "id": "0x82"
            }
          },
          {
            "id": "0x2f4f4f",
            "type": [
              "Donation"
            ],
            "amount": 10,
            "project": {
              "id": "0x82"
            }
          }
        ],
        "category": {
          "id": "0x1408c2",
          "type": [
            "Category"
          ],
          "name": "Literacy & Language, Math & Science"
        }
      }
    ]
  },
  "extensions": {
    "server_latency": {
      "parsing_ns": 82220,
      "processing_ns": 2294120,
      "encoding_ns": 264387,
      "assign_timestamp_ns": 1445391,
      "total_ns": 4146848
    },
    "txn": {
      "start_ts": 170245
    },
    "metrics": {
      "num_uids": {
        "Category.name": 4,
        "Donation.amount": 52,
        "Donation.date": 52,
        "Donation.project": 52,
        "Project.category": 5,
        "Project.donations": 5,
        "Project.title": 5,
        "_total": 302,
        "dgraph.type": 61,
        "uid": 66
      }
    }
  }
}
"""
