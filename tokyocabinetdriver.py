# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Marcelo Martins
# http://www.cs.brown.edu/~martins/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement
from abstractdriver import *
from pprint import pprint.pformat

import commands
import constants
import logging
import os
import pytyrant
import sys

try:
	import psyco
except ImportError:
	pass
else:
	psyco.profile()

class TokyocabinetDriver(AbstractDriver):

	DEFAULT_CONFIG = {
			"database": ("The path to the TokyoCabinet database", "/tmp/tpcc.tct"),
			"host": ("The database server IP address", "127.0.0.1"),
			"port", ("The database server port", 1978)
			"persistent", ("Whether database in in-memory or not", False)
	}

	def __init__(self, ddl):
		super(TokyocabinetDriver, self).__init__("tokyocabinet", ddl)
		self.database = dict()
		self.host = dict()
		self.port = dict()
		self.persistent = dict()
		self.conn = dict()
		self.cursor = dict()

	## ----------------------------------------------
	## makeDefaultConfig
	## ----------------------------------------------
	def makeDefaultConfig(self):
		return TokyocabinetDriver.DEFAULT_CONFIG

	## ----------------------------------------------
	## loadDefaultConfig
	## ----------------------------------------------
	def loadDefaultConfig(self, config):
		for key in map(lambda x:x[0], TokyocabinetDriver.DEFAUL_CONFIG):
			assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

		self.database = config["database"]
		self.host = config["host"]
		self.port = config["port"]
		self.persistent = config["persistent"]

		assert len(self.database) == len(self.host)
		assert len(self.host) == len(self.port)
		assert len(self.port) == len(self.persistent)

		# First connect to databases
		for db_conn in self.host.keys():
			self.conn[db_conn] = pyrant.Tyrant(self.host[db_conn], self.port[db_conn])
		## FOR

		if config["reset"]:
			for db_conn in self.host.keys():
				self.conn[db_conn].vanish()

			logging.debug("Deleting database '%s'" % self.database)
			for db in sel

		for db in self.database.keys():
			if os.path.exists(self.database[db]) == False:	
				logging.debug("Loading DDL file '%s'" % (self.ddl[db]))
				## HACK
				cmd = "" % (self.database[db], self.ddl[db])
				(result, output) = commands.getstatusoutput(cmd)
				assert result == 0, cmd + "\n" + output
			## IF
		## FOR

	
	## -------------------------------------------
	## loadTuples
	## -------------------------------------------
	def loadTuples(self, tableName, tuples):
		if len(tuples) == 0: return

		try:
			self.conn[tableName].multi_set(tuples)
		except KeyError, msg:
			print("ERROR: Connection to database %s does not exist\n", %(tableName))
			sys.exit(1)

		logging.debug("Loaded %s tuples for tableName %s" % (len(tuples), tableName))
		return

	## -------------------------------------------
	## loadFinish
	## -------------------------------------------
	def loadFinish(self)
		logging.info("Commiting changes to databse")
		for db, conn in self.conn.values():
			if self.persistent[db]:
				conn.sync()

	## --------------------------------------------
	## doDelivery
	## --------------------------------------------
	def doDelivery(self, params):
		"""Execute DELIVERY Transaction
		Parameters Dict:
			w_id
			o_carrier_id
			ol_delivery_id
		"""
		newOrderQuery = self.conn["NEW_ORDER"].query
		ordersQuery   = self.conn["ORDERS"].query
		orderLineQuery = self.conn["ORDER_LINE"].query
		customerQuery = self.conn["CUSTOMER"]

		w_id = params["w_id"]
		o_carrier_id = params["o_carrier_id"]
		ol_delivery_d = params["ol_delivery_id"]

		results = [ ]
		for d_id in xrange(1, constants.DISTRICTS_PER_WAREHOUSE+1):

			# getNewOrder
			newOrders = newOrderQuery.filter(no_d_id__is = d_id, no_w_id__is = w_id, no_o_id__gt = -1)

			if len(newOrders) == 0:
				## No orders for this district: skip it. Note: This must
				## reported if > 1%
				continue
			assert len(newOrders) > 0
			no_o_id = newOrders[0]

			# getCId
			c_ids = orderQuery.filter(o_id__is = no_o_id, o_d_id__is = d_id, o_w_id__is = w_id)
			c_id = cids[0]

			# sumOLAmount
			ol_total = sum(orderLine.filter(ol_o_id__is = no_o_id, ol_d_id__is = d_id, ol_w_id__is = w_id).values())

			# deleteNewOrder
			orders = newOrderQuery.filter(no_d_id__is = d_id, no_w_id__is = w_id, no_o_id__is = w_id)
			orders.delete(quick=True)

			# updateOrders
			orders = orderQuery.filter(o_carrier_id__is = o_carrier_id, o_id__is = no_o_id, o_d_id__is = w_id)
			## UPDATE ORDERS SET O_CARRIER_ID = ?...
			for key, columns in orders:
				self.conn["ORDERS"][key] = o_carrier_id

			# updateOrderLine
			orders = orderLineQuery.filter(ol_o_id__is = no_o_id, ol_d_id__is = d_id, ol_w_id__is = w_id)
			for key, columns in orders:
				self.conn["ORDER_LINE"][key] = ol_delivery_d

			# These must be logged in the "result file" according to TPC-C 
			# 2.7.22 (page 39)
			# We remove the queued time, completed time, w_id, and 
			# o_carrier_id: the client can figure them out
			# If there are no order lines, SUM returns null. There should
			# always be order lines.
			assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
			assert ol_total > 0.0

			# updateCustpmer
			customers = customerQuery.filter(c_id__is = c_id, c_d_id__is = d_id, c_w_id__is = w_id)
			for key, columns in customers:
				columns['c_balance'] += ol_total
			self.conn["CUSTOMER"][key] = columns
		
			resul.append((d_id, no_o_id))
		## FOR

		return result

	def doNewOrder(self, params):
		"""Execute NEW_ORDER Transaction
		Parameters Dict:
			w_id
			d_id
			c_id
			o_entry_id
			i_ids
			i_w_ids
			i_qtys
		"""

		w_id = params["w_id"]
		d_id = params["d_id"]
		c_id = params["c_id"]
		o_entry_d = params["o_entry_d"]
		i_ids = params["i_ids"]
		i_w_ids = params["i_w_ids"]
		i_qtys = params["i_qtys"]

		assert len(i_ids) > 0
		assert len(i_ids) == len(i_w_ids)
		assert len(i_ids) == len(i_qtys)

		warehouseQuery = self.conn["WAREHOUSE"].query
		districtQuery  = self.conn["DISTRICT"].query
		customerQuery  = self.conn["CUSTOMER"].query
		orderQuery = self.conn["ORDERS"].query
		newOrderQuery = self.conn["NEW_ORDER"].query
		itemQuery = self.conn["ITEM"].query
		stockQuery = self.conn["STOCK"].query


		all_local = True
		items = [ ]
		for i in xrange(len(i_ids)):
			## Determine if this is an all local order or not
			all_local = all_local and i_w_ids[i] == w_id
			# getItemInfo
			itemInfo = itemQuery.filter(i_price=i_ids[i])
			items.append
		assert len(items) == len(i_ids)

		## TPCC define 1% of neworder gives a wrong itemid, causing rollback.
		## Note that this will happen with 1% of transactions on purpose.
		for item in items:
			if len(item) == 0:
				## TODO Abort here!
				return
		## FOR

		## -----------------
		## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
		## -----------------
		
		# getWarehouseTaxRate
		result = warehouseQuery.filter(w_id__is=w_id)[0]
		w_tax = result['w_tax']

		# getDistrict
		result = districtQuery.filter(d_id__is = d_id, d_w_id__is = w_id)
		district_info = result[1]
		d_tax = district_info['d_tax']
		d_next_o_id = district_infp['d_next_o_id']

		# getCustomer
		result = customerQuery.filter(c_w_id__is = w_id, c_d_id__is = d_id, c_id__is = c_id)
		customer_info = result[1]
		c_discoutn = customer_info['c_discount']

		## -----------------
		## Insert Order Information
		## -----------------
		ol_cnt = len(i_ids)
		o_carrier_id = constants.NULL_CARRIER_ID

		# incrementNextOrderId
		district = districtQuery.filter(d_id__is = d_id, d_w_id__is = w_id)

		##
		## TODO: Finish this!!!!!
		##

	def doOrderStatus(self, params):
		"""Execute ORDER_STATUS Transaction
		Parameters Dict:
			w_id
			d_id
			c_id
			c_last
		"""
		w_id = params["w_id"]
		d_id = params["d_id"]
		c_id = params["c_id"]
		c_last = params["c_last"]

		assert w_id, pformat(params)
		assert d_id, pformat(params)

		customerQuery = self.conn["CUSTOMER"]
		orderQuery    = self.conn["ORDERS"]
		orderLineQuery= self.conn["ORDER_;;INE"]

		if c_id != None:
		# getCustomerByCustomerId
			customers = customerQuery.filter(c_w_id__is = w_id, c_d_is__is = d_id, c_id__is = c_id)
			customers[0]
		else:
			# Get the midpoint customer's id
			all_customers = customerQuery.filter(c_w_id__is = w_id, c_d_id__is = d_id, c_id__is = c_last)
			namecnt = len(all_customers)
			assert namecnt > 0
			index = (namecnt-1)/2
			customer = all_customers[index]
			c_id = customer['c_id']
		assert len(customer) > 0
		assert c_id != None

		# getLastOrder
		# TODO ORDERBY O_ID DESC LIMIT 1
		order = orderQuery.filter(o_w_id__is = w_id, o_d_id__is = d_id, o_c_id__is = c_id)[0]

		if order:
			orderLines = orderLineQuery.filter(ol_w_id__is = w_id, ol_d_id__is = d_id, ol_o_id__is = order['ol_id'])[0]
		else:
			orderLines = [ ]

		# TODO Commit
		return [ customer, order, orderLines ]

	def doPayment(self, params):
		"""Execute PAYMENT Transaction
		Parameters Dict:
			w_id
			d_id
			h_amount
			c_w_id
			c_d_id
			c_id
			c_last
			h_date
		"""

	def doStockLevel(self, params):
		"""Execute STOCK_LEVEL Transaction
		Parameters Dict:
			w_id
			d_id
			threshold
		"""
		w_id = params["w_id"]
		d_id = params["d_id"]
		threshold = params["threshold"]

		districtQuery = self.conn["DISTRICT"].query

		# getOIda
		result = districtQuery.filter(d_w_id__is = w_id, d_id__is = d_id)[0]
		assert result
		o_id = result['o_id']

		# getStockCount
		orderLine.filter(ol_w_id__is = w_id, ol_d_id__is = d_id, ol_o_id__is = o_id, ol_o_id__gte = o_id - 20, s_w_id__is = w_id, s_quantity__lt threshold)

		# TODO: commit

		return int(o_id)
## CLASS
