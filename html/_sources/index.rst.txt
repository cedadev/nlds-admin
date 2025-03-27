.. NLDS Admin documentation master file, created by
   sphinx-quickstart on Wed Mar 12 11:36:49 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

NLDS Admin documentation
========================

The Near-Line Data Store (NLDS) is a multi-tiered storage solution that uses 
Object Storage as a front end cache to a tape library. It catalogs the data as 
it is ingested and permits multiple versions of files. It has a microservice 
architecture using a message broker to communicate between the parts.

Interaction with NLDS is via a HTTP API, with a Python library and command-line 
client provided to users for programmatic or interactive use.
Authentication is carried out for each user using OAuth2.  

Due to this authentication, it is not possible for an administrator to see another 
user's NLDS jobs by going through either the command-line client or the Python library.

However, due to the interaction with NLDS being passed along a line of message queues,
with individual worker processes attached to each queue, it is possible to inject a 
message into a queue that is further along the line than the authentication server, in 
the form of a Remote-Procedure Call (RPC).  To be authorised to do this, the caller
of the RPC must have permission to write to the RabbitMQ exchange for NLDS.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation.rst
   config.rst
   command_ref.rst
   license.rst


Acknowledgements
================

NLDS was developed at the `Centre for Environmental Data Analysis <https://www.ceda.ac.uk>`_
with support from the ESiWACE2 project. The project ESiWACE2 has received 
funding from the European Union's Horizon 2020 research and innovation programme
under grant agreement No 823988. 

.. image:: _images/esiwace2.png
   :width: 300
   :alt: ESiWACE2 Project Logo

.. image:: _images/ceda.png
   :width: 300
   :alt: CEDA Logo


NLDS is Open-Source software with a BSD-2 Clause License.  The license can be
read :ref:`here <license>`.