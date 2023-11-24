from managers.mapping_persistence_manager import MappingPersistenceManager

mpm = MappingPersistenceManager(".", True)

tmp = mpm.msgpack.load("mapping")
tp = mpm.pickle.load("mapping")
tvj = mpm.vanilla.load("mapping")
tvj_ftp = mpm.vanilla.load("compoundsmapping")

print(f"Pickle: Loaded 1374 in {str(tp[1].delta())}")
print(f"MsgPack: Loaded 1374 in {str(tmp[1].delta())}")
print(f"VanillaJSON: Loaded 1374 in {str(tvj[1].delta())}")

print("skunto")
