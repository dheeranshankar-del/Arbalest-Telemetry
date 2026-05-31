"""
============================================================
 ARBALEST ROCKETRY
 GROUND STATION  v1.0
 Mission Control Software — Real-Time Telemetry Dashboard
 Designed for Arbalest Rocketry competition operations
============================================================

Architecture:
  UDPReceiver      – Dedicated thread listening on UDP port 5005
  TelemetryBus     – Thread-safe data bus (lock-free latest-packet store)
  MainWindow       – PyQt6 top-level window, 60 Hz refresh timer
  ArtificialHorizon– Custom QPainter widget
  RocketGL         – PyQtGraph GLViewWidget 3-D model
  TelemetryCard    – Reusable animated value card
  AccelPanel       – Bar-chart style accelerometer readout
  ScrollChart      – PyQtGraph PlotWidget rolling buffer
  SystemHealth     – Live health/status panel

UDP packet format:
  binary IMU datagram decoded by UDPReceiver
"""

import sys
import math
import time
import socket
import threading
import collections
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QGridLayout, QLabel, QFrame, QSizePolicy
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, pyqtSignal, QRectF, QPointF, QSize
)
from PyQt6.QtGui import (
    QPainter, QColor, QPen, QBrush, QFont, QFontMetrics,
    QLinearGradient, QRadialGradient, QPainterPath, QPolygonF,
    QPalette
)

import pyqtgraph as pg
import pyqtgraph.opengl as gl


# ─────────────────────────────────────────────────────────────
#  DESIGN CONSTANTS
# ─────────────────────────────────────────────────────────────
CLR_BG          = "#050608"   # Near-black operations background
CLR_PANEL       = "#0B0F14"   # Panel background
CLR_PANEL_BORDER = "#263142"  # Panel border
CLR_CYAN        = "#2D7DFF"   # Arbalest blue accent
CLR_CYAN_DIM    = "#174A8C"   # Dimmed Arbalest blue
CLR_BLUE        = "#4DA3FF"   # Secondary accent
CLR_GREEN       = "#23D18B"   # Status good
CLR_AMBER       = "#F5C542"   # Status warn
CLR_RED         = "#FF4D5E"   # Status critical
CLR_TEXT_PRI    = "#F4F7FB"   # Primary text
CLR_TEXT_SEC    = "#8996A7"   # Secondary text
CLR_GRID        = "#1A222E"   # Chart grid lines
CLR_DISABLED    = "#566273"   # Disabled future sensor text

FONT_MONO  = "Cascadia Mono"
FONT_UI    = "Segoe UI"

HIST_LEN   = 500    # scrolling chart history length
UDP_PORT   = 5005
REFRESH_HZ = 60     # GUI refresh rate


# ─────────────────────────────────────────────────────────────
#  TELEMETRY DATA STRUCTURES
# ─────────────────────────────────────────────────────────────
@dataclass
class TelemetryPacket:
    roll:  float = 0.0
    pitch: float = 0.0
    yaw:   float = 0.0
    ax:    float = 0.0
    ay:    float = 0.0
    az:    float = 9.81
    timestamp: float = field(default_factory=time.time)


class TelemetryBus:
    """
    Thread-safe data bus.
    Writer thread calls update(); GUI thread calls latest().
    Uses a simple lock — hold time is microseconds.
    """
    def __init__(self):
        self._lock   = threading.Lock()
        self._packet = TelemetryPacket()
        self._count  = 0          # total packets received
        self._t0     = time.time()
        self._rate   = 0.0        # packets / second
        self._interval_ms = 0.0
        self._jitter_ms = 0.0
        self._connected = False

        # Rate estimation — ring of recent timestamps
        self._ts_ring: collections.deque = collections.deque(maxlen=200)
        self._dt_ring: collections.deque = collections.deque(maxlen=120)

    def update(self, pkt: TelemetryPacket):
        now = time.time()
        with self._lock:
            if self._ts_ring:
                interval_ms = (now - self._ts_ring[-1]) * 1000.0
                self._interval_ms = interval_ms
                self._dt_ring.append(interval_ms)
                if len(self._dt_ring) > 1:
                    avg = sum(self._dt_ring) / len(self._dt_ring)
                    var = sum((dt - avg) ** 2 for dt in self._dt_ring) / len(self._dt_ring)
                    self._jitter_ms = math.sqrt(var)
            pkt.timestamp = now
            self._packet    = pkt
            self._count    += 1
            self._connected = True
            self._ts_ring.append(now)
            # Estimate rate from packets in last second
            cutoff = now - 1.0
            recent = sum(1 for t in self._ts_ring if t > cutoff)
            self._rate = float(recent)

    def latest(self) -> TelemetryPacket:
        with self._lock:
            return self._packet

    @property
    def connected(self) -> bool:
        with self._lock:
            if not self._connected:
                return False
            # Stale if no packet in last 3 s
            age = time.time() - self._packet.timestamp
            return age < 3.0

    @property
    def packet_rate(self) -> float:
        with self._lock:
            return self._rate

    @property
    def packet_count(self) -> int:
        with self._lock:
            return self._count

    @property
    def packet_interval_ms(self) -> float:
        with self._lock:
            return self._interval_ms

    @property
    def packet_jitter_ms(self) -> float:
        with self._lock:
            return self._jitter_ms


# ─────────────────────────────────────────────────────────────
#  UDP RECEIVER THREAD
# ─────────────────────────────────────────────────────────────
class UDPReceiver(QThread):
    """
    Non-blocking UDP listener running in a dedicated QThread.
    Emits no Qt signals — writes directly to TelemetryBus to
    avoid Qt event-loop overhead at 100 Hz.
    """
    import struct

    PACKET_MAGIC = 0xAA55
    PACKET_FORMAT = "<HIffffffQ"
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)

    def __init__(self, bus: TelemetryBus, port: int = UDP_PORT):
        super().__init__()
        self.bus  = bus
        self.port = port
        self._running = True

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(0.5)
        try:
            sock.bind(("", self.port))
        except OSError as e:
            print(f"[UDP] Bind error on port {self.port}: {e}")
            return

        print(f"[UDP] Listening on port {self.port}")

        while self._running:
            try:
                data, _ = sock.recvfrom(256)
                self._parse(data)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[UDP] Recv error: {e}")

        sock.close()

    def _parse(self, raw: bytes):
        if len(raw) != self.PACKET_SIZE:
            return

        magic, _t, roll, pitch, yaw, ax, ay, az, _mac_send_us = self.struct.unpack(
            self.PACKET_FORMAT,
            raw,
        )
        if magic != self.PACKET_MAGIC:
            return

        pkt = TelemetryPacket(
            roll  = roll,
            pitch = pitch,
            yaw   = yaw,
            ax    = ax,
            ay    = ay,
            az    = az,
        )
        self.bus.update(pkt)

    def stop(self):
        self._running = False
        self.wait(2000)


# ─────────────────────────────────────────────────────────────
#  STYLED PANEL BASE
# ─────────────────────────────────────────────────────────────
def make_panel(title: str = "", accent: str = CLR_CYAN) -> QFrame:
    """Factory: dark bordered panel with optional header label."""
    frame = QFrame()
    frame.setStyleSheet(f"""
        QFrame {{
            background-color: {CLR_PANEL};
            border: 1px solid {CLR_PANEL_BORDER};
            border-radius: 8px;
        }}
    """)
    return frame


class HeaderLabel(QLabel):
    """Small all-caps section header in cyan."""
    def __init__(self, text: str):
        super().__init__(text.upper())
        self.setStyleSheet(f"""
            QLabel {{
                color: {CLR_CYAN};
                font-family: '{FONT_MONO}';
                font-size: 9px;
                font-weight: bold;
                letter-spacing: 1px;
                padding: 4px 6px 2px 6px;
                background: transparent;
                border: none;
            }}
        """)


# ─────────────────────────────────────────────────────────────
#  ARTIFICIAL HORIZON
# ─────────────────────────────────────────────────────────────
class ArtificialHorizon(QWidget):
    """
    Classic attitude indicator rendered entirely with QPainter.
    Animates smoothly at 60 Hz — no image assets required.
    Displays pitch (horizon tilt) and roll (bank angle).
    """
    def __init__(self):
        super().__init__()
        self._roll  = 0.0
        self._pitch = 0.0
        self.setMinimumSize(180, 180)
        self.setMaximumSize(220, 220)
        self.setSizePolicy(QSizePolicy.Policy.Preferred,
                           QSizePolicy.Policy.Preferred)

    def set_attitude(self, roll: float, pitch: float):
        self._roll  = roll
        self._pitch = pitch
        self.update()

    def paintEvent(self, event):
        w, h = self.width(), self.height()
        size  = min(w, h)
        cx, cy = w / 2, h / 2
        r = size / 2 - 6

        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        # ── Clip to circle ──────────────────────────────────
        clip = QPainterPath()
        clip.addEllipse(QPointF(cx, cy), r, r)
        p.setClipPath(clip)

        # ── Sky / Ground split with pitch offset ────────────
        p.save()
        p.translate(cx, cy)
        p.rotate(-self._roll)

        pitch_px = (self._pitch / 90.0) * r

        # Sky gradient
        sky_grad = QLinearGradient(0, -r, 0, r)
        sky_grad.setColorAt(0.0, QColor("#0D47A1"))
        sky_grad.setColorAt(1.0, QColor("#1976D2"))
        p.setBrush(QBrush(sky_grad))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawRect(int(-r), int(-r * 2), int(r * 2), int(r * 2 + pitch_px))

        # Ground gradient
        gnd_grad = QLinearGradient(0, -r, 0, r)
        gnd_grad.setColorAt(0.0, QColor("#5D4037"))
        gnd_grad.setColorAt(1.0, QColor("#3E2723"))
        p.setBrush(QBrush(gnd_grad))
        p.drawRect(int(-r), int(pitch_px), int(r * 2), int(r * 2))

        # Horizon line
        p.setPen(QPen(QColor("#FFFFFF"), 2))
        p.drawLine(int(-r), int(pitch_px), int(r), int(pitch_px))

        # Pitch ladder lines
        p.setFont(QFont(FONT_MONO, 7))
        p.setPen(QPen(QColor(255, 255, 255, 180), 1))
        for deg in range(-80, 81, 10):
            if deg == 0:
                continue
            y = pitch_px - (deg / 90.0) * r
            lw = r * 0.35 if deg % 20 == 0 else r * 0.2
            p.drawLine(int(-lw), int(y), int(lw), int(y))
            if deg % 20 == 0:
                p.drawText(QPointF(lw + 4, y + 4), f"{abs(deg)}")

        p.restore()

        # ── Horizon reference marks (fixed, not rotating) ───
        p.save()
        p.translate(cx, cy)
        p.setPen(QPen(QColor(CLR_CYAN), 2.5))
        # Fixed horizontal reference bars
        p.drawLine(int(-r * 0.55), 0, int(-r * 0.15), 0)
        p.drawLine(int(r * 0.15),  0, int(r * 0.55),  0)
        # Centre dot
        p.setBrush(QBrush(QColor(CLR_CYAN)))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QPointF(0, 0), 5, 5)
        p.restore()

        # ── Roll arc & tick marks ────────────────────────────
        p.save()
        p.translate(cx, cy)
        arc_r = r - 12
        pen = QPen(QColor(CLR_TEXT_PRI), 1)
        p.setPen(pen)
        for angle in [-60, -45, -30, -20, -10, 0, 10, 20, 30, 45, 60]:
            rad = math.radians(angle - 90)
            tick_len = 10 if angle % 30 == 0 else 6
            x1 = arc_r * math.cos(rad)
            y1 = arc_r * math.sin(rad)
            x2 = (arc_r - tick_len) * math.cos(rad)
            y2 = (arc_r - tick_len) * math.sin(rad)
            p.drawLine(QPointF(x1, y1), QPointF(x2, y2))

        # Roll pointer triangle
        p.save()
        p.rotate(-self._roll)
        tri = QPolygonF([
            QPointF(0, -(arc_r - 2)),
            QPointF(-6, -(arc_r - 16)),
            QPointF(6, -(arc_r - 16)),
        ])
        p.setBrush(QBrush(QColor(CLR_CYAN)))
        p.setPen(QPen(QColor(CLR_CYAN), 1))
        p.drawPolygon(tri)
        p.restore()
        p.restore()

        # ── Remove clip; draw outer bezel ───────────────────
        p.setClipping(False)
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.setPen(QPen(QColor(CLR_CYAN_DIM), 2))
        p.drawEllipse(QPointF(cx, cy), r, r)

        # Outer dark ring
        p.setPen(QPen(QColor(CLR_PANEL_BORDER), 5))
        p.drawEllipse(QPointF(cx, cy), r + 4, r + 4)

        # ── Numeric overlays ────────────────────────────────
        p.setFont(QFont(FONT_MONO, 9, QFont.Weight.Bold))
        p.setPen(QPen(QColor(CLR_CYAN)))
        p.drawText(QRectF(cx - 40, cy + r - 36, 80, 20),
                   Qt.AlignmentFlag.AlignCenter,
                   f"P {self._pitch:+.1f}°  R {self._roll:+.1f}°")

        p.end()


# ─────────────────────────────────────────────────────────────
#  3-D ROCKET GL WIDGET
# ─────────────────────────────────────────────────────────────
class RocketGL(gl.GLViewWidget):
    """
    PyQtGraph OpenGL widget rendering a simplified rocket model
    composed of GLMeshItems. Attitude updated via set_attitude().
    """
    def __init__(self):
        super().__init__()
        self.setBackgroundColor(CLR_BG)
        self.opts["distance"] = 4.8
        self.opts["fov"]      = 34
        self.opts["elevation"] = 18
        self.opts["azimuth"] = -35
        self._roll  = 0.0
        self._pitch = 0.0
        self._yaw   = 0.0
        self._flight_visual_items = []

        self._build_rocket()

    # ── Build rocket geometry ────────────────────────────────
    def _build_rocket(self):
        body_verts, body_faces, body_colors = self._cylinder(
            radius=0.18, height=2.7, segments=32,
            color=(0.82, 0.84, 0.82, 1.0))

        nose_verts, nose_faces, nose_colors = self._cone(
            radius=0.19, height=0.62, segments=32, z_off=1.35,
            color=(0.96, 0.97, 0.95, 1.0))

        stripe_verts, stripe_faces, stripe_colors = self._body_stripe(
            radius=0.184, z_min=-1.18, z_max=1.14, angle_deg=-24,
            width_deg=10, color=(0.05, 0.36, 0.95, 1.0))

        fin_meshes = []
        for angle in [0, 90, 180, 270]:
            fv, ff, fc = self._fin(angle_deg=angle)
            fin_meshes.append((fv, ff, fc))

        nozzle_v, nozzle_f, nozzle_c = self._cylinder(
            radius=0.1, height=0.18, segments=20,
            z_off=-1.44, color=(0.08, 0.085, 0.095, 1.0))

        all_parts = (
            [(body_verts, body_faces, body_colors),
             (stripe_verts, stripe_faces, stripe_colors),
             (nose_verts, nose_faces, nose_colors),
             (nozzle_v,   nozzle_f,   nozzle_c)]
            + fin_meshes
        )

        self._items = []
        for verts, faces, colors in all_parts:
            md   = gl.MeshData(vertexes=verts, faces=faces,
                               faceColors=colors)
            mesh = gl.GLMeshItem(meshdata=md, smooth=True,
                                 drawEdges=False, shader="edgeHilight",
                                 glOptions="opaque")
            self.addItem(mesh)
            self._items.append(mesh)

        grid = gl.GLGridItem()
        grid.setSize(6, 6)
        grid.setSpacing(0.5, 0.5)
        grid.setColor((70, 82, 96, 65))
        grid.translate(0, 0, -1.7)
        self.addItem(grid)

    def _cylinder(self, radius, height, segments=24,
                  z_off=0.0, color=(1,1,1,1)):
        verts = []
        faces = []
        colors = []
        h2 = height / 2

        for i in range(segments):
            a = 2 * math.pi * i / segments
            verts.append([radius * math.cos(a),
                          radius * math.sin(a),
                          -h2 + z_off])
        for i in range(segments):
            a = 2 * math.pi * i / segments
            verts.append([radius * math.cos(a),
                          radius * math.sin(a),
                           h2 + z_off])

        verts = np.array(verts, dtype=float)

        for i in range(segments):
            n  = (i + 1) % segments
            faces.append([i, n, segments + i])
            faces.append([n, segments + n, segments + i])
            colors.append(list(color))
            colors.append(list(color))

        return verts, np.array(faces), np.array(colors, dtype=float)

    def _cone(self, radius, height, segments=24,
              z_off=0.0, color=(1,1,1,1)):
        verts = []
        faces = []
        colors = []

        apex = [0, 0, z_off + height]
        for i in range(segments):
            a = 2 * math.pi * i / segments
            verts.append([radius * math.cos(a),
                          radius * math.sin(a),
                          z_off])
        verts.append(apex)
        verts = np.array(verts, dtype=float)

        tip = len(verts) - 1
        for i in range(segments):
            n = (i + 1) % segments
            faces.append([i, n, tip])
            colors.append(list(color))

        return verts, np.array(faces), np.array(colors, dtype=float)

    def _body_stripe(self, radius, z_min, z_max, angle_deg=0,
                     width_deg=20, color=(1,1,1,1)):
        centre = math.radians(angle_deg)
        half = math.radians(width_deg) / 2
        angles = [centre - half, centre + half]
        verts = np.array([
            [radius * math.cos(angles[0]), radius * math.sin(angles[0]), z_min],
            [radius * math.cos(angles[1]), radius * math.sin(angles[1]), z_min],
            [radius * math.cos(angles[0]), radius * math.sin(angles[0]), z_max],
            [radius * math.cos(angles[1]), radius * math.sin(angles[1]), z_max],
        ], dtype=float)
        faces = np.array([[0, 1, 2], [1, 3, 2]])
        colors = np.array([list(color), list(color)], dtype=float)
        return verts, faces, colors

    def _fin(self, angle_deg=0):
        ar = math.radians(angle_deg)
        cos_a, sin_a = math.cos(ar), math.sin(ar)
        rb = 0.18  # body radius

        pts = [
            [rb * cos_a,        rb * sin_a,        -1.12],
            [(rb+0.42)*cos_a,   (rb+0.42)*sin_a,   -1.32],
            [(rb+0.32)*cos_a,   (rb+0.32)*sin_a,   -0.55],
            [rb * cos_a,        rb * sin_a,        -0.35],
        ]
        verts = np.array(pts, dtype=float)
        faces = np.array([[0, 1, 2], [0, 2, 3]])
        color = (0.09, 0.095, 0.105, 1.0)
        colors = np.array([list(color), list(color)], dtype=float)
        return verts, faces, colors

    def set_flight_visualization(self, altitude_m: Optional[float] = None):
        # Reserved for future altitude/position rendering when those sensors exist.
        pass

    # ── Public update ────────────────────────────────────────
    def set_attitude(self, roll: float, pitch: float, yaw: float):
        self._roll  = roll
        self._pitch = pitch
        self._yaw   = yaw
        self._apply_rotation()

    def _apply_rotation(self):
        """Reset and reapply Euler rotations to all rocket parts."""
        for item in self._items:
            item.resetTransform()
            # Apply in ZYX order: yaw → pitch → roll
            item.rotate(self._yaw,   0, 0, 1)
            item.rotate(self._pitch, 0, 1, 0)
            item.rotate(self._roll,  1, 0, 0)


# ─────────────────────────────────────────────────────────────
#  TELEMETRY VALUE CARD
# ─────────────────────────────────────────────────────────────
class TelemetryCard(QWidget):
    """
    Single large-format readout card for one telemetry value.
    Shows label, numeric value with unit, and a colour-coded
    level bar.
    """
    def __init__(self, label: str, unit: str = "°",
                 v_range: tuple = (-180, 180)):
        super().__init__()
        self._label   = label
        self._unit    = unit
        self._range   = v_range
        self._value   = 0.0

        self.setMinimumHeight(86)
        self.setStyleSheet(f"""
            background: #0D131A;
            border: 1px solid {CLR_PANEL_BORDER};
            border-radius: 8px;
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 9, 14, 9)
        layout.setSpacing(2)

        self._hdr = QLabel(label.upper())
        self._hdr.setStyleSheet(f"""
            color: {CLR_TEXT_SEC};
            font-family: '{FONT_MONO}';
            font-size: 9px;
            font-weight: bold;
            letter-spacing: 1px;
        """)

        self._val_label = QLabel("0.00°")
        self._val_label.setStyleSheet(f"""
            color: {CLR_CYAN};
            font-family: '{FONT_MONO}';
            font-size: 27px;
            font-weight: bold;
        """)

        layout.addWidget(self._hdr)
        layout.addWidget(self._val_label)
        layout.addStretch()

    def set_value(self, v: float):
        self._value = v
        self._val_label.setText(f"{v:+.2f}{self._unit}")

        # Colour: green → amber → red based on magnitude
        frac = abs(v) / max(abs(self._range[0]), abs(self._range[1]))
        if frac < 0.5:
            self._val_label.setStyleSheet(f"""
                color: {CLR_CYAN};
                font-family: '{FONT_MONO}';
                font-size: 27px; font-weight: bold;
            """)
        elif frac < 0.8:
            self._val_label.setStyleSheet(f"""
                color: {CLR_AMBER};
                font-family: '{FONT_MONO}';
                font-size: 27px; font-weight: bold;
            """)
        else:
            self._val_label.setStyleSheet(f"""
                color: {CLR_RED};
                font-family: '{FONT_MONO}';
                font-size: 27px; font-weight: bold;
            """)


# ─────────────────────────────────────────────────────────────
#  ACCELEROMETER BAR PANEL
# ─────────────────────────────────────────────────────────────
class FutureTelemetryPanel(QWidget):
    """Disabled placeholders for sensors not present in the current packet."""
    def __init__(self):
        super().__init__()
        layout = QGridLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setHorizontalSpacing(14)
        layout.setVerticalSpacing(6)

        items = ["ALTITUDE", "VELOCITY", "FLIGHT STATE", "GPS STATUS"]
        for idx, name in enumerate(items):
            row = idx // 2
            col = (idx % 2) * 2

            lbl = QLabel(name)
            lbl.setStyleSheet(f"""
                color: {CLR_DISABLED};
                font-family: '{FONT_MONO}';
                font-size: 9px;
                font-weight: bold;
                letter-spacing: 1px;
            """)

            val = QLabel("N/A")
            val.setEnabled(False)
            val.setAlignment(Qt.AlignmentFlag.AlignRight)
            val.setStyleSheet(f"""
                color: {CLR_DISABLED};
                font-family: '{FONT_MONO}';
                font-size: 11px;
                font-weight: bold;
            """)

            layout.addWidget(lbl, row, col)
            layout.addWidget(val, row, col + 1)


class TimingPerformancePanel(QWidget):
    """GUI readout for link timing and display cadence."""
    def __init__(self):
        super().__init__()
        layout = QGridLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setHorizontalSpacing(10)
        layout.setVerticalSpacing(5)

        self._rows = {}
        labels = [
            "Packet Rate (Hz)",
            "Packet Interval (ms)",
            "Packet Jitter (ms)",
            "GUI FPS",
            "Last Packet Age (ms)",
            "Estimated Display Delay (ms)",
        ]

        for row, label in enumerate(labels):
            lbl = QLabel(label.upper())
            lbl.setStyleSheet(f"""
                color: {CLR_TEXT_SEC};
                font-family: '{FONT_MONO}';
                font-size: 8px;
                font-weight: bold;
                letter-spacing: 1px;
            """)

            val = QLabel("N/A")
            val.setAlignment(Qt.AlignmentFlag.AlignRight)
            val.setStyleSheet(f"""
                color: {CLR_TEXT_PRI};
                font-family: '{FONT_MONO}';
                font-size: 10px;
                font-weight: bold;
            """)

            layout.addWidget(lbl, row, 0)
            layout.addWidget(val, row, 1)
            self._rows[label] = val

    def update_metrics(self, packet_rate: float,
                       interval_ms: Optional[float],
                       jitter_ms: Optional[float],
                       gui_fps: float,
                       packet_age_ms: Optional[float],
                       display_delay_ms: Optional[float]):
        self._rows["Packet Rate (Hz)"].setText(f"{packet_rate:.1f}")
        self._rows["GUI FPS"].setText(f"{gui_fps:.1f}")

        if packet_age_ms is None or display_delay_ms is None:
            for label in self._rows:
                if label not in ["Packet Rate (Hz)", "GUI FPS"]:
                    self._rows[label].setText("N/A")
            return

        for label, value in [
            ("Packet Interval (ms)", interval_ms),
            ("Packet Jitter (ms)", jitter_ms),
        ]:
            if value is None:
                self._rows[label].setText("N/A")
            else:
                self._rows[label].setText(f"{value:.2f}")

        self._rows["Last Packet Age (ms)"].setText(f"{packet_age_ms:.1f}")
        self._rows["Estimated Display Delay (ms)"].setText(
            f"{display_delay_ms:.1f}"
        )


class AccelPanel(QWidget):
    """
    Horizontal bar-chart readout for 3-axis accelerometer.
    Each axis rendered with a filled bar and numeric label.
    """
    def __init__(self):
        super().__init__()
        self._ax = self._ay = 0.0
        self._az = 9.81
        self.setMinimumHeight(160)
        self.setSizePolicy(QSizePolicy.Policy.Expanding,
                           QSizePolicy.Policy.Preferred)

    def set_values(self, ax: float, ay: float, az: float):
        self._ax, self._ay, self._az = ax, ay, az
        self.update()

    def paintEvent(self, event):
        w, h = self.width(), self.height()
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.fillRect(0, 0, w, h, QColor(CLR_PANEL))

        axes   = [("AX", self._ax), ("AY", self._ay), ("AZ", self._az)]
        max_g  = 20.0
        bar_h  = 24
        gap    = (h - 20 - len(axes) * bar_h) // (len(axes) + 1)
        label_w = 36

        p.setFont(QFont(FONT_MONO, 9, QFont.Weight.Bold))

        for i, (name, val) in enumerate(axes):
            y = 10 + gap + i * (bar_h + gap)

            # Axis label
            p.setPen(QPen(QColor(CLR_TEXT_SEC)))
            p.drawText(QRectF(6, y, label_w, bar_h),
                       Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft,
                       name)

            bar_x = label_w + 10
            bar_w = w - bar_x - 70

            # Background track
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QBrush(QColor(CLR_GRID)))
            p.drawRoundedRect(int(bar_x), int(y + 4),
                              int(bar_w), int(bar_h - 8), 3, 3)

            # Centre tick (zero)
            cx = bar_x + bar_w / 2
            p.setPen(QPen(QColor(CLR_PANEL_BORDER), 1))
            p.drawLine(QPointF(cx, y + 2), QPointF(cx, y + bar_h - 2))

            # Value bar
            frac = max(-1.0, min(1.0, val / max_g))
            fill_w = abs(frac) * (bar_w / 2)
            fill_x = cx if frac >= 0 else cx - fill_w

            colour = CLR_CYAN if abs(val) < 15 else CLR_RED
            grad = QLinearGradient(fill_x, 0, fill_x + fill_w, 0)
            grad.setColorAt(0, QColor(colour + "66"))
            grad.setColorAt(1, QColor(colour))
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(QBrush(grad))
            p.drawRoundedRect(int(fill_x), int(y + 4),
                              max(2, int(fill_w)), int(bar_h - 8), 3, 3)

            # Numeric value
            p.setPen(QPen(QColor(CLR_TEXT_PRI)))
            p.setFont(QFont(FONT_MONO, 9, QFont.Weight.Bold))
            p.drawText(
                QRectF(w - 66, y, 62, bar_h),
                Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight,
                f"{val:+.2f}g"
            )

        p.end()


# ─────────────────────────────────────────────────────────────
#  SCROLLING CHART
# ─────────────────────────────────────────────────────────────
class ScrollChart(pg.PlotWidget):
    """
    Rolling-buffer PyQtGraph chart for one telemetry channel.
    Configured for minimal CPU overhead: no auto-range,
    pre-allocated arrays.
    """
    def __init__(self, title: str, color: str,
                 y_range: tuple = (-180, 180), unit: str = "°"):
        super().__init__()
        self._buf = collections.deque([0.0] * HIST_LEN, maxlen=HIST_LEN)
        self._x   = np.arange(HIST_LEN, dtype=float)
        self._color = color

        # Style
        self.setBackground(CLR_BG)
        self.showGrid(x=False, y=True, alpha=0.15)
        self.setLabel("left",  title, units=unit,
                      color=CLR_TEXT_SEC, size="9pt")
        self.setRange(yRange=y_range, padding=0)
        self.setMenuEnabled(False)
        self.setMouseEnabled(x=False, y=False)

        # Grid pen
        self.getAxis("left").setPen(pg.mkPen(CLR_PANEL_BORDER))
        self.getAxis("bottom").setPen(pg.mkPen(CLR_PANEL_BORDER))
        self.getAxis("left").setTextPen(pg.mkPen(CLR_TEXT_SEC))
        self.getAxis("bottom").setTextPen(pg.mkPen(CLR_TEXT_SEC))

        self._curve = self.plot(
            self._x, np.zeros(HIST_LEN),
            pen=pg.mkPen(color=color, width=1.5)
        )
        self.setMinimumHeight(100)

    def push(self, value: float):
        self._buf.append(value)

    def refresh(self):
        self._curve.setData(self._x, np.array(self._buf))


# ─────────────────────────────────────────────────────────────
#  CONNECTION STATUS WIDGET
# ─────────────────────────────────────────────────────────────
class ConnectionStatus(QWidget):
    """Animated dot + text connection indicator."""
    def __init__(self):
        super().__init__()
        self._connected = False
        self._blink     = False
        self.setFixedHeight(30)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(8)

        self._dot_lbl = QLabel("●")
        self._dot_lbl.setText("LINK")
        self._dot_lbl.setFixedWidth(34)
        self._dot_lbl.setFont(QFont(FONT_MONO, 8, QFont.Weight.Bold))

        self._txt_lbl = QLabel("ARBALEST LINK  NO SIGNAL")
        self._txt_lbl.setFont(QFont(FONT_MONO, 9, QFont.Weight.Bold))
        self._txt_lbl.setStyleSheet(f"color: {CLR_TEXT_SEC};")

        layout.addWidget(self._dot_lbl)
        layout.addWidget(self._txt_lbl)
        layout.addStretch()

        # Blink timer
        self._blink_timer = QTimer(self)
        self._blink_timer.timeout.connect(self._do_blink)
        self._blink_timer.start(500)

    def set_connected(self, conn: bool, rate: float = 0.0):
        self._connected = conn
        if conn:
            self._txt_lbl.setText(f"ARBALEST LINK  {rate:.0f} Hz")
            self._txt_lbl.setStyleSheet(f"color: {CLR_GREEN};")
        else:
            self._txt_lbl.setText("ARBALEST LINK  NO SIGNAL")
            self._txt_lbl.setStyleSheet(f"color: {CLR_TEXT_SEC};")

    def _do_blink(self):
        self._blink = not self._blink
        if self._connected:
            col = CLR_GREEN if self._blink else "#006633"
        else:
            col = CLR_AMBER if self._blink else CLR_TEXT_SEC
        self._dot_lbl.setStyleSheet(f"color: {col};")


# ─────────────────────────────────────────────────────────────
#  SYSTEM HEALTH PANEL
# ─────────────────────────────────────────────────────────────
class SystemHealthPanel(QWidget):
    """Compact grid of system status rows."""
    def __init__(self):
        super().__init__()
        layout = QGridLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(4)

        self._rows = {}
        items = [
            ("UDP LINK",    CLR_AMBER),
            ("PACKET RATE", CLR_GREEN),
            ("TOTAL PKT",   CLR_GREEN),
            ("ACCEL NORM",  CLR_GREEN),
            ("UPTIME",      CLR_GREEN),
        ]
        for i, (name, _) in enumerate(items):
            lbl = QLabel(name)
            lbl.setStyleSheet(f"""
                color: {CLR_TEXT_SEC};
                font-family: '{FONT_MONO}';
                font-size: 9px;
            """)
            val = QLabel("—")
            val.setStyleSheet(f"""
                color: {CLR_TEXT_PRI};
                font-family: '{FONT_MONO}';
                font-size: 9px;
                font-weight: bold;
            """)
            val.setAlignment(Qt.AlignmentFlag.AlignRight)
            layout.addWidget(lbl, i, 0)
            layout.addWidget(val, i, 1)
            self._rows[name] = val

        self._t0 = time.time()

    def update_health(self, bus: TelemetryBus, pkt: TelemetryPacket):
        conn = bus.connected
        rate = bus.packet_rate

        self._rows["UDP LINK"].setText("CONNECTED" if conn else "LOST")
        self._rows["UDP LINK"].setStyleSheet(
            f"color: {CLR_GREEN if conn else CLR_RED}; "
            f"font-family: '{FONT_MONO}'; font-size: 9px; font-weight: bold;")

        self._rows["PACKET RATE"].setText(f"{rate:.1f} pkt/s")

        self._rows["TOTAL PKT"].setText(str(bus.packet_count))

        accel_norm = math.sqrt(pkt.ax**2 + pkt.ay**2 + pkt.az**2)
        self._rows["ACCEL NORM"].setText(f"{accel_norm:.2f} m/s²")

        elapsed = time.time() - self._t0
        m, s = divmod(int(elapsed), 60)
        h, m = divmod(m, 60)
        self._rows["UPTIME"].setText(f"{h:02d}:{m:02d}:{s:02d}")


# ─────────────────────────────────────────────────────────────
#  MISSION CLOCK HEADER
# ─────────────────────────────────────────────────────────────
class MissionHeader(QWidget):
    """Top banner: mission name, clock, branding."""
    def __init__(self):
        super().__init__()
        self._t0 = time.time()
        self.setFixedHeight(52)
        self.setStyleSheet(f"background: #03070C;")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 6, 16, 6)

        # Left — logo / mission
        left = QVBoxLayout()
        left.setSpacing(0)
        title = QLabel("ARBALEST ROCKETRY")
        title.setStyleSheet(f"""
            color: {CLR_CYAN};
            font-family: '{FONT_MONO}';
            font-size: 15px;
            font-weight: bold;
            letter-spacing: 3px;
        """)
        sub = QLabel("GROUND STATION  |  IMU ATTITUDE  |  v1.0")
        sub.setStyleSheet(f"""
            color: {CLR_TEXT_SEC};
            font-family: '{FONT_MONO}';
            font-size: 8px;
            letter-spacing: 2px;
        """)
        left.addWidget(title)
        left.addWidget(sub)

        # Centre — MET clock
        self._met = QLabel("MET  T+00:00:00")
        self._met.setStyleSheet(f"""
            color: {CLR_GREEN};
            font-family: '{FONT_MONO}';
            font-size: 16px;
            font-weight: bold;
            letter-spacing: 2px;
        """)
        self._met.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Right — status pill
        self._status = QLabel("STANDBY")
        self._status.setStyleSheet(f"""
            color: {CLR_AMBER};
            font-family: '{FONT_MONO}';
            font-size: 10px;
            font-weight: bold;
            letter-spacing: 2px;
        """)
        self._status.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        layout.addLayout(left, 3)
        layout.addWidget(self._met, 2)
        layout.addWidget(self._status, 2)

        # Separator line at bottom
        sep = QFrame(self)
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"background: {CLR_PANEL_BORDER};")
        sep.setGeometry(0, self.height() - 1, 9999, 1)

    def tick(self, connected: bool):
        elapsed = time.time() - self._t0
        m, s = divmod(int(elapsed), 60)
        h, m = divmod(m, 60)
        self._met.setText(f"MET  T+{h:02d}:{m:02d}:{s:02d}")

        if connected:
            self._status.setText("LIVE TELEMETRY")
            self._status.setStyleSheet(f"""
                color: {CLR_GREEN};
                font-family: '{FONT_MONO}';
                font-size: 10px;
                font-weight: bold; letter-spacing: 2px;
            """)
        else:
            self._status.setText("STANDBY")
            self._status.setStyleSheet(f"""
                color: {CLR_AMBER};
                font-family: '{FONT_MONO}';
                font-size: 10px;
                font-weight: bold; letter-spacing: 2px;
            """)


# ─────────────────────────────────────────────────────────────
#  MAIN WINDOW
# ─────────────────────────────────────────────────────────────
class MainWindow(QMainWindow):
    """
    Top-level window.  Layout:
        Header (full width)
        ┌───────────────────────────────────────────────┐
        │  LEFT          │  CENTRE       │  RIGHT       │
        │  ─ Horizon     │  ─ Roll card  │  ─ Accel     │
        │  ─ 3D rocket   │  ─ Pitch card │  ─ Charts    │
        │  ─ Conn status │  ─ Yaw card   │  ─ Health    │
        │                │  ─ Pkt rate   │              │
        └───────────────────────────────────────────────┘

    A single 60 Hz QTimer drives all GUI updates.
    The UDPReceiver runs in its own QThread and writes to TelemetryBus.
    """

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ARBALEST ROCKETRY - GROUND STATION")
        self.resize(1440, 900)

        # ── Dark application palette ─────────────────────────
        self._apply_palette()

        # ── Telemetry back-end ───────────────────────────────
        self.bus = TelemetryBus()
        self.udp = UDPReceiver(self.bus)
        self.udp.start()

        # ── Central widget ───────────────────────────────────
        central = QWidget()
        central.setStyleSheet(f"background: {CLR_BG};")
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Header
        self.header = MissionHeader()
        root.addWidget(self.header)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"background: {CLR_PANEL_BORDER}; max-height: 1px;")
        root.addWidget(sep)

        # Main 3-column body
        body = QWidget()
        body.setStyleSheet(f"background: {CLR_BG};")
        body_layout = QHBoxLayout(body)
        body_layout.setContentsMargins(12, 10, 12, 10)
        body_layout.setSpacing(10)

        body_layout.addWidget(self._build_left(), 2)
        body_layout.addWidget(self._build_rocket_focus(), 5)
        body_layout.addWidget(self._build_right(), 3)

        root.addWidget(body, 1)

        # Status bar
        self.statusBar().setStyleSheet(
            f"background: #03070C; color: {CLR_TEXT_SEC}; "
            f"font-family: '{FONT_MONO}'; font-size: 9px;")
        self.statusBar().showMessage(
            f"  ARBALEST ROCKETRY GROUND STATION   UDP:{UDP_PORT}   "
            f"REFRESH: {REFRESH_HZ} Hz   "
            "TELEMETRY: IMU ATTITUDE")

        # ── 60 Hz refresh timer ──────────────────────────────
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(1000 // REFRESH_HZ)

        # Scrolling chart tick (every frame)
        self._frame = 0
        self._frame_times: collections.deque = collections.deque(maxlen=120)

    # ── Panel builders ────────────────────────────────────────
    def _build_left(self) -> QWidget:
        w = QWidget()
        w.setStyleSheet(f"background: {CLR_BG};")
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        layout.addWidget(HeaderLabel("ATTITUDE"))

        self.card_roll  = TelemetryCard("ROLL",  "°", (-180, 180))
        self.card_pitch = TelemetryCard("PITCH", "°", (-90, 90))
        self.card_yaw   = TelemetryCard("YAW",   "°", (-180, 180))

        for card in [self.card_roll, self.card_pitch, self.card_yaw]:
            layout.addWidget(card, 1)

        hz_panel = make_panel("ATTITUDE")
        hz_layout = QVBoxLayout(hz_panel)
        hz_layout.setContentsMargins(10, 8, 10, 10)
        hz_layout.setSpacing(6)
        hz_layout.addWidget(HeaderLabel("ARTIFICIAL HORIZON"))
        self.horizon = ArtificialHorizon()
        hz_layout.addWidget(self.horizon, 0, Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(hz_panel, 2)

        stats_panel = make_panel()
        stats_layout = QGridLayout(stats_panel)
        stats_layout.setContentsMargins(12, 8, 12, 10)
        stats_layout.setSpacing(6)

        def stat_pair(label, row):
            lbl = QLabel(label)
            lbl.setStyleSheet(f"""
                color: {CLR_TEXT_SEC};
                font-family: '{FONT_MONO}'; font-size: 9px;
            """)
            val = QLabel("-")
            val.setStyleSheet(f"""
                color: {CLR_TEXT_PRI};
                font-family: '{FONT_MONO}'; font-size: 12px; font-weight: bold;
            """)
            val.setAlignment(Qt.AlignmentFlag.AlignRight)
            stats_layout.addWidget(lbl, row, 0)
            stats_layout.addWidget(val, row, 1)
            return val

        stats_layout.addWidget(HeaderLabel("LINK METRICS"), 0, 0, 1, 2)
        self._rate_val = stat_pair("PKT RATE", 1)
        self._count_val = stat_pair("TOTAL PKT", 2)
        self._latency_val = stat_pair("SIGNAL", 3)
        layout.addWidget(stats_panel, 2)

        conn_panel = make_panel()
        conn_layout = QVBoxLayout(conn_panel)
        conn_layout.setContentsMargins(8, 4, 8, 4)
        self.conn_status = ConnectionStatus()
        conn_layout.addWidget(self.conn_status)
        layout.addWidget(conn_panel, 1)

        return w

    def _build_rocket_focus(self) -> QWidget:
        w = QWidget()
        w.setStyleSheet(f"background: {CLR_BG};")
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        gl_panel = make_panel("3D VIEW")
        gl_layout = QVBoxLayout(gl_panel)
        gl_layout.setContentsMargins(10, 8, 10, 10)
        gl_layout.setSpacing(6)
        gl_layout.addWidget(HeaderLabel("PRIMARY ROCKET ATTITUDE"))
        self.rocket_gl = RocketGL()
        self.rocket_gl.setMinimumHeight(500)
        self.rocket_gl.setSizePolicy(QSizePolicy.Policy.Expanding,
                                     QSizePolicy.Policy.Expanding)
        gl_layout.addWidget(self.rocket_gl, 1)
        layout.addWidget(gl_panel, 8)

        future_panel = make_panel()
        future_layout = QVBoxLayout(future_panel)
        future_layout.setContentsMargins(10, 6, 10, 8)
        future_layout.setSpacing(4)
        future_layout.addWidget(HeaderLabel("FUTURE FLIGHT DATA"))
        self.future_telemetry = FutureTelemetryPanel()
        future_layout.addWidget(self.future_telemetry)
        layout.addWidget(future_panel, 1)

        return w

    def _build_right(self) -> QWidget:
        w = QWidget()
        w.setStyleSheet(f"background: {CLR_BG};")
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        # Accelerometer bars
        accel_panel = make_panel()
        accel_layout = QVBoxLayout(accel_panel)
        accel_layout.setContentsMargins(10, 8, 10, 10)
        accel_layout.setSpacing(6)
        accel_layout.addWidget(HeaderLabel("ACCELEROMETER"))
        self.accel_panel = AccelPanel()
        accel_layout.addWidget(self.accel_panel)
        layout.addWidget(accel_panel, 2)

        timing_panel = make_panel()
        timing_layout = QVBoxLayout(timing_panel)
        timing_layout.setContentsMargins(10, 6, 10, 8)
        timing_layout.setSpacing(4)
        timing_layout.addWidget(HeaderLabel("TIMING & PERFORMANCE"))
        self.timing_perf = TimingPerformancePanel()
        timing_layout.addWidget(self.timing_perf)
        layout.addWidget(timing_panel, 2)

        # Live charts
        charts_panel = make_panel()
        charts_layout = QVBoxLayout(charts_panel)
        charts_layout.setContentsMargins(10, 8, 10, 6)
        charts_layout.setSpacing(5)
        charts_layout.addWidget(HeaderLabel("LIVE TELEMETRY - ROLLING 5 s"))

        self.chart_roll  = ScrollChart("ROLL",  CLR_CYAN,   (-180, 180))
        self.chart_pitch = ScrollChart("PITCH", CLR_GREEN,  (-90,  90))
        self.chart_yaw   = ScrollChart("YAW",   "#7B61FF",  (-180, 180))
        self.chart_az    = ScrollChart("AZ",    CLR_AMBER,  (-20,  20), "m/s²")

        for ch in [self.chart_roll, self.chart_pitch,
                   self.chart_yaw, self.chart_az]:
            charts_layout.addWidget(ch, 1)

        layout.addWidget(charts_panel, 5)

        # System health
        health_panel = make_panel()
        health_layout = QVBoxLayout(health_panel)
        health_layout.setContentsMargins(10, 6, 10, 8)
        health_layout.setSpacing(0)
        health_layout.addWidget(HeaderLabel("SYSTEM HEALTH"))
        self.health = SystemHealthPanel()
        health_layout.addWidget(self.health)
        layout.addWidget(health_panel, 2)

        return w

    # ── 60 Hz refresh slot ─────────────────────────────────────
    def _refresh(self):
        """Called 60× per second. Reads latest telemetry, updates UI."""
        frame_now = time.time()
        self._frame_times.append(frame_now)
        if len(self._frame_times) > 1:
            span = self._frame_times[-1] - self._frame_times[0]
            gui_fps = (len(self._frame_times) - 1) / span if span > 0 else 0.0
        else:
            gui_fps = 0.0

        pkt  = self.bus.latest()
        conn = self.bus.connected
        rate = self.bus.packet_rate
        packet_count = self.bus.packet_count
        interval_ms = self.bus.packet_interval_ms if packet_count > 1 else None
        jitter_ms = self.bus.packet_jitter_ms if packet_count > 2 else None
        packet_age_ms = None
        display_delay_ms = None
        if packet_count > 0:
            packet_age_ms = max(0.0, (frame_now - pkt.timestamp) * 1000.0)
            display_delay_ms = packet_age_ms

        # Attitude displays
        self.horizon.set_attitude(pkt.roll, pkt.pitch)
        self.rocket_gl.set_attitude(pkt.roll, pkt.pitch, pkt.yaw)

        # Cards
        self.card_roll.set_value(pkt.roll)
        self.card_pitch.set_value(pkt.pitch)
        self.card_yaw.set_value(pkt.yaw)

        # Accelerometer
        self.accel_panel.set_values(pkt.ax, pkt.ay, pkt.az)

        # Charts — push new sample, refresh every frame
        self.chart_roll.push(pkt.roll)
        self.chart_pitch.push(pkt.pitch)
        self.chart_yaw.push(pkt.yaw)
        self.chart_az.push(pkt.az)

        self.chart_roll.refresh()
        self.chart_pitch.refresh()
        self.chart_yaw.refresh()
        self.chart_az.refresh()

        # Connection & link metrics
        self.conn_status.set_connected(conn, rate)
        self._rate_val.setText(f"{rate:.1f} pkt/s")
        self._count_val.setText(str(packet_count))
        sig = "EXCELLENT" if conn and rate > 80 else \
              "GOOD"      if conn and rate > 40 else \
              "WEAK"      if conn else "NO SIGNAL"
        sig_col = {
            "EXCELLENT": CLR_GREEN,
            "GOOD":      CLR_CYAN,
            "WEAK":      CLR_AMBER,
            "NO SIGNAL": CLR_RED,
        }[sig]
        self._latency_val.setText(sig)
        self._latency_val.setStyleSheet(
            f"color: {sig_col}; font-family: '{FONT_MONO}'; "
            "font-size: 12px; font-weight: bold;")

        self.timing_perf.update_metrics(
            rate,
            interval_ms,
            jitter_ms,
            gui_fps,
            packet_age_ms,
            display_delay_ms,
        )

        # Header clock
        self.header.tick(conn)

        # System health
        self.health.update_health(self.bus, pkt)

        self._frame += 1

    # ── Dark palette ────────────────────────────────────────────
    def _apply_palette(self):
        pal = self.palette()
        pal.setColor(QPalette.ColorRole.Window,        QColor(CLR_BG))
        pal.setColor(QPalette.ColorRole.WindowText,    QColor(CLR_TEXT_PRI))
        pal.setColor(QPalette.ColorRole.Base,          QColor(CLR_PANEL))
        pal.setColor(QPalette.ColorRole.Text,          QColor(CLR_TEXT_PRI))
        pal.setColor(QPalette.ColorRole.Button,        QColor(CLR_PANEL))
        pal.setColor(QPalette.ColorRole.ButtonText,    QColor(CLR_TEXT_PRI))
        pal.setColor(QPalette.ColorRole.Highlight,     QColor(CLR_CYAN))
        pal.setColor(QPalette.ColorRole.HighlightedText, QColor(CLR_BG))
        QApplication.instance().setPalette(pal)

    def closeEvent(self, event):
        """Clean shutdown: stop UDP thread before closing."""
        self.udp.stop()
        super().closeEvent(event)


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────
def main():
    # High-DPI and OpenGL hints
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_ShareOpenGLContexts)

    app = QApplication(sys.argv)
    app.setApplicationName("ARBALEST ROCKETRY GROUND STATION")
    app.setOrganizationName("Arbalest Rocketry")

    # Global stylesheet
    app.setStyleSheet(f"""
        QToolTip {{
            background: {CLR_PANEL};
            color: {CLR_TEXT_PRI};
            border: 1px solid {CLR_CYAN_DIM};
            font-family: '{FONT_MONO}';
            font-size: 9px;
        }}
        QScrollBar {{
            background: {CLR_PANEL};
            width: 6px;
        }}
        QScrollBar::handle {{
            background: {CLR_CYAN_DIM};
            border-radius: 3px;
        }}
    """)

    # PyQtGraph global config
    pg.setConfigOptions(antialias=True, foreground=CLR_TEXT_PRI,
                        background=CLR_BG)

    win = MainWindow()
    win.show()

    print("=" * 60)
    print(" ARBALEST ROCKETRY")
    print(" GROUND STATION  v1.0")
    print("=" * 60)
    print(f" UDP receiver:  port {UDP_PORT}")
    print(f" GUI refresh:   {REFRESH_HZ} Hz")
    print(" Packet format: binary IMU datagram")
    print()
    print(" Test with:")
    print(f"   python3 sim_transmitter.py")
    print("=" * 60)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
