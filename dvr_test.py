from can.listener import Listener
import tkinter as tk
import can
import threading
import time
from queue import Queue

# Setup CAN bus
bus = can.interface.Bus(channel='can0', bustype='socketcan', bitrate=250000)

# DVR Inputs
active_inputs = set()

def send_inputs():
    can_id = 0x18FF78BD
    input_byte = sum((1 << (i - 1)) for i in active_inputs if 1 <= i <= 8)
    data = [input_byte] + [0xFF] * 7
    msg = can.Message(arbitration_id=can_id, data=data, is_extended_id=True)
    try:
        bus.send(msg)
        input_status.set(f"Inputs sent: {input_byte:08b}")
    except can.CanError as e:
        input_status.set(f"CAN Error: {e}")

def toggle_input(i, var):
    if var.get():
        active_inputs.add(i)
    else:
        active_inputs.discard(i)
    send_inputs()

# --- Additional group for GPIO-like GUI simulation ---
import lgpio

def toggle_gpio_input(i, var):
    # Logical GPIO Input → Physical GPIO Pin Map
    gpio_output_map = {
        1: 4,
        2: 5,
        3: 6,
        4: 23,
        5: 24,
        6: 9,
        7: 10,
        8: 11
    }

    pin = gpio_output_map.get(i)
    if pin is not None:
        try:
            h = lgpio.gpiochip_open(0)
            lgpio.gpio_claim_output(h, pin, var.get())
            lgpio.gpio_write(h, pin, var.get())
            lgpio.gpiochip_close(h)
            log_queue.put(f"[GPIO] Set GPIO pin {pin} to {'HIGH' if var.get() else 'LOW'}")
        except Exception as e:
            log_queue.put(f"[GPIO ERROR] Failed to toggle GPIO {pin}: {e}")
    else:
        log_queue.put(f"[GPIO ERROR] No mapping for logical GPIO {i}")

# DM1 Fault Codes
def send_dm1():
    try:
        spn = int(spn_entry.get())
        fmi = int(fmi_entry.get())
        oc = int(oc_entry.get())
        sa = int(sa_entry.get())
        lamp = lamp_var.get()
        dtc = [
            spn & 0xFF,
            ((spn >> 8) & 0x07) | ((fmi & 0x1F) << 3),
            (oc & 0x7F),
            0xFF
        ]
        data = [lamp, 0x00] + dtc + [0xFF] * 2
        can_id = 0x18FECA00 | (sa & 0xFF)
        msg = can.Message(arbitration_id=can_id, data=data[:8], is_extended_id=True)
        # Add log output here
        print(f"[DEBUG] Sending engine DM1: ID=0x{msg.arbitration_id:X}, Data={msg.data.hex()}")
        log_queue.put(f"[DEBUG] DM1 Sent: {msg.data.hex().upper()}")
        
        bus.send(msg)
        dm1_status.set(f"DM1 sent: SPN={spn}, FMI={fmi}, SA={sa}")
    except Exception as e:
        dm1_status.set(f"Error: {e}")

# Speed Broadcast
running_speed = False
def send_speed_loop():
    global running_speed
    while running_speed:
        try:
            mph = int(speed_entry.get())
            kmh = int(mph * 1.60934)
            byte3 = kmh & 0xFF
            data = [0xFF, 0xFF, byte3] + [0xFF] * 5
            msg = can.Message(arbitration_id=0x18FEF100, data=data, is_extended_id=True)
            bus.send(msg)
            speed_status.set(f"Sent: {mph} MPH (≈{kmh} km/h) → Byte 3: 0x{byte3:02X}")
        except Exception as e:
            speed_status.set(f"Speed Error: {e}")
        time.sleep(0.1)

def toggle_speed():
    global running_speed
    running_speed = not running_speed
    if running_speed:
        threading.Thread(target=send_speed_loop, daemon=True).start()
        toggle_button.config(text="Stop Speed")
    else:
        toggle_button.config(text="Start Speed")
        speed_status.set("Speed sending stopped.")

# CAN Logger
log_running = False
log_queue = Queue()

def logger_loop():
    global log_running
    while log_running:
        try:
            msg = bus.recv(timeout=0.1)
            if msg:
                log_queue.put(f"[{msg.timestamp:.3f}] {msg.arbitration_id:08X}  {msg.data.hex(' ').upper()}")
        except Exception as e:
            log_queue.put(f"[Logger error] {e}")

def update_log_display():
    try:
        while not log_queue.empty():
            msg = log_queue.get_nowait()
            log_text.insert(tk.END, msg + '\n')
            log_text.see(tk.END)
    except Exception as e:
        log_text.insert(tk.END, f"[UI Error] {e}\n")
    finally:
        root.after(100, update_log_display)

def toggle_logger():
    global log_running
    log_running = not log_running
    if log_running:
        log_button.config(text="Stop Logger")
        threading.Thread(target=logger_loop, daemon=True).start()
    else:
        log_button.config(text="Start Logger")

def clear_all():
    global running_speed, log_running
    running_speed = False
    log_running = False
    toggle_button.config(text="Start Speed")
    log_button.config(text="Start Logger")
    speed_status.set("Speed stopped.")
    dm1_status.set("")
    input_status.set("")
    log_text.delete(1.0, tk.END)
    active_inputs.clear()
    for var in input_vars + gpio_input_vars:
        var.set(0)

# GUI Layout
root = tk.Tk()
root.title("CAN Bus DVR Tool")

# DVR Inputs Frame
input_frame = tk.LabelFrame(root, text="DVR J1939 Inputs")
input_frame.grid(row=0, column=0, padx=10, pady=10)
input_vars = []
for i in range(1, 9):
    var = tk.IntVar()
    input_vars.append(var)
    cb = tk.Checkbutton(input_frame, text=f"Input {i}", variable=var,
                        command=lambda i=i, v=var: toggle_input(i, v))
    cb.grid(row=(i-1)//4, column=(i-1)%4, sticky='w')
input_status = tk.StringVar()
tk.Label(input_frame, textvariable=input_status).grid(row=3, column=0, columnspan=4, pady=5)

# GPIO Inputs Frame (simulated)
gpio_frame = tk.LabelFrame(root, text="GPIO Inputs (Simulated)")
gpio_frame.grid(row=1, column=0, padx=10, pady=10)
gpio_input_vars = []
for i in range(1, 9):
    var = tk.IntVar()
    gpio_input_vars.append(var)
    cb = tk.Checkbutton(gpio_frame, text=f"GPIO {i}", variable=var,
                        command=lambda i=i, v=var: toggle_gpio_input(i, v))
    cb.grid(row=(i-1)//4, column=(i-1)%4, sticky='w')

# DM1 Fault Frame
dm1_frame = tk.LabelFrame(root, text="DM1 Fault Codes")
dm1_frame.grid(row=0, column=1, padx=10, pady=10)
tk.Label(dm1_frame, text="SPN:").grid(row=0, column=0)
spn_entry = tk.Entry(dm1_frame); spn_entry.insert(0, "792"); spn_entry.grid(row=0, column=1)
tk.Label(dm1_frame, text="FMI:").grid(row=1, column=0)
fmi_entry = tk.Entry(dm1_frame); fmi_entry.insert(0, "1"); fmi_entry.grid(row=1, column=1)
tk.Label(dm1_frame, text="OC:").grid(row=2, column=0)
oc_entry = tk.Entry(dm1_frame); oc_entry.insert(0, "1"); oc_entry.grid(row=2, column=1)
tk.Label(dm1_frame, text="SA:").grid(row=3, column=0)
sa_entry = tk.Entry(dm1_frame); sa_entry.insert(0, "0"); sa_entry.grid(row=3, column=1)
tk.Label(dm1_frame, text="Lamp:").grid(row=4, column=0)
lamp_var = tk.IntVar(); lamp_menu = tk.OptionMenu(dm1_frame, lamp_var, 0x00, 0x10, 0x20)
lamp_var.set(0x20); lamp_menu.grid(row=4, column=1)
tk.Button(dm1_frame, text="Send DM1", command=send_dm1).grid(row=5, column=0, columnspan=2, pady=5)
dm1_status = tk.StringVar(); tk.Label(dm1_frame, textvariable=dm1_status).grid(row=6, column=0, columnspan=2)

# Speed Frame
speed_frame = tk.LabelFrame(root, text="Vehicle Speed")
speed_frame.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky='ew')
tk.Label(speed_frame, text="MPH:").grid(row=0, column=0)
speed_entry = tk.Entry(speed_frame); speed_entry.insert(0, "50"); speed_entry.grid(row=0, column=1)
toggle_button = tk.Button(speed_frame, text="Start Speed", command=toggle_speed)
toggle_button.grid(row=0, column=2)
speed_status = tk.StringVar(); tk.Label(speed_frame, textvariable=speed_status).grid(row=1, column=0, columnspan=3)

# Logger Frame
log_queue.put("[TEST] Logger display setup complete")
log_frame = tk.LabelFrame(root, text="CAN Log")
log_frame.grid(row=3, column=0, columnspan=2, padx=10, pady=10, sticky='ew')
log_button = tk.Button(log_frame, text="Start Logger", command=toggle_logger)
log_button.grid(row=0, column=0, padx=5, pady=5)
clear_button = tk.Button(log_frame, text="Clear / Stop All", command=clear_all)
clear_button.grid(row=0, column=1, padx=5)
log_text = tk.Text(log_frame, height=10, width=80)
log_text.grid(row=1, column=0, columnspan=2)

# Start logger display updates
update_log_display()

# Start GUI loop
root.mainloop()
