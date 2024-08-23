import paho.mqtt.client as mqtt
# import keyboard
import time
import json
import sys
import re
from queue import Queue

global short_pause
short_pause = 0.3

####################################
def on_msg ( client, userdata, msg ):
	# print ( "msg rcvd ", str ( msg.payload.decode ( "utf-8" ) ) )
	# print ( "msg topic ", msg.topic )
	q.put ( msg )
####################################

####################################
def jcmd_process ( jmsg ):
  if jmsg [ "fdbk" ] != None:
    print ( jmsg [ "fdbk" ] )
    json_lst_to_pub = json.dumps ( jmsg [ "fdbk" ] )
    client.publish ( JNEW_TOPIC, json_lst_to_pub, qos=0, retain=False )
####################################

# Main program

# Analisis de invocacion
my_name = sys.argv[0]
args_qty = len(sys.argv)

if args_qty < 2 or args_qty > 4:
	print (
		f"python3 {my_name}: Broker_IP_Address <dev_mac [def:11-22-33-44-55-66]> <dev_type [def:spkr]\n"
		f"(Ej: {my_name} 192.168.1.170)"
	)
	exit(1)

broker_addr = sys.argv[ 1 ]

DEFAULT_MAC = "11-22-33-44-55-66"
DEFAULT_TYPE = "spkr"
global dev_mac, dev_type
dev_mac = DEFAULT_MAC
dev_type = DEFAULT_TYPE

if args_qty >= 3:
	dev_mac = sys.argv[ 2 ]

if args_qty == 4:
	dev_type = sys.argv[ 3 ]

#definiciones de MQTT
mytransport = "tcp" # or 'websockets'
JNEW_TOPIC = dev_type + "/jnew/" + dev_mac
JCMD_TOPIC_BEG = dev_type + "/jcmd/"

# Cola que recibe las novedades
q=Queue()

# Creo el cliente MQTT
client = mqtt.Client(
			client_id="simul_dev",
			transport=mytransport,
			protocol=mqtt.MQTTv311,
			clean_session=True,
			userdata=None
)

# Funcion de callback que va a procesar los mensajes
client.on_message = on_msg

# Conexion
client.connect ( broker_addr, port=1883 )

print ( f"Subscribing broker {broker_addr} as " + dev_type + "\n" )
client.subscribe ( JCMD_TOPIC_BEG + dev_mac, qos=0 )

dict_to_pub = {
	"new": "online"
}
json_lst_to_pub = json.dumps ( dict_to_pub )
client.publish ( JNEW_TOPIC, json_lst_to_pub, qos=0, retain=False )
print ( "<=", json_lst_to_pub )

# Comienzo de loop (crea un thread para que funcione el cliente mqtt en background
client.loop_start()

while True:

	# Espero una novedad
	while q.empty():
		#if keyboard.is_pressed("q"):
		#	print("q pressed, ending test")
		#	exit( 0 )
		time.sleep(.1)

	# Vino una novedad, extraigo topic y payload
	msg = q.get()
	topic = msg.topic
	pyld = str(msg.payload.decode("utf-8"))

	# Verifico que el topic comience con "<dev_type>/jcmd/"
	mobj = re.search ( JCMD_TOPIC_BEG, topic  )
	if mobj == None:
		print ( "wrong topic" + topic )
		continue

	# Decodifico el payload en json
	jmsg = json.loads( pyld )
	print ( "=>", jmsg )

	# Proceso la novedad
	jcmd_process ( jmsg )
