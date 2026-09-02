#!/usr/bin/env python3
"""
Flight Tracker - Smart IP Bypass (Compact & Enhanced)
"""

import requests, time, random, socket, json, os
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# Colors
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"; B = "\033[1m"; X = "\033[0m"

class SmartBypass:
    def __init__(self):
        self.proxies = []
        self.last_call = 0
        self.cache = {}
        self.uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0"
        ]
    
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        # Rate limiting
        elapsed = time.time() - self.last_call
        if elapsed < 2:
            time.sleep(2 - elapsed)
        
        headers = {"User-Agent": random.choice(self.uas), "Accept": "application/json"}
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
        
        # Try direct first
        try:
            r = requests.get(url, headers=headers, timeout=12, **kwargs)
            if r.status_code == 200:
                self.last_call = time.time()
                return r
            if r.status_code == 429:
                print(f"  {Y}⚠️ Rate limit - switching proxy{X}")
        except:
            pass
        
        # Try proxies
        if not self.proxies:
            self._fetch_proxies()
        
        for proxy in self.proxies[:5]:
            try:
                r = requests.get(url, proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"}, 
                               headers=headers, timeout=10, **kwargs)
                if r.status_code == 200:
                    print(f"  {G}✓ Proxy success: {proxy}{X}")
                    self.last_call = time.time()
                    return r
            except:
                continue
        return None
    
    def _fetch_proxies(self):
        # Use environment proxies only — avoid untrusted public proxy lists
        http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        if http_proxy or https_proxy:
            proxy = https_proxy or http_proxy
            # extract host:port from URL like http://host:port
            proxy_addr = proxy.replace("http://", "").replace("https://", "").rstrip("/")
            self.proxies = [proxy_addr]
        else:
            self.proxies = []

def print_box(title: str, color: str = C):
    print(f"\n{color}{'═'*60}{X}")
    print(f"{B}{color}  ✈️  {title}{X}")
    print(f"{color}{'═'*60}{X}")

def main():
    print_box("FLIGHT TRACKER - SMART BYPASS", C)
    
    # Check internet
    try:
        socket.create_connection(("8.8.8.8", 53), 2)
        print(f"  {G}✅ Internet: Connected{X}")
    except:
        print(f"  {R}❌ No internet{X}")
        return
    
    bypass = SmartBypass()
    
    # Load credentials if exist
    auth = None
    cred_paths = [
        os.path.join(os.path.dirname(__file__), "..", "api", "credentials.json"),
        os.path.join(os.path.dirname(__file__), "credentials.json"),
        "api/credentials.json",
    ]
    for cred_file in cred_paths:
        if os.path.exists(cred_file):
            try:
                with open(cred_file) as f:
                    d = json.load(f)
                    user = d.get("clientId") or d.get("username")
                    pwd  = d.get("clientSecret") or d.get("password")
                    if user and pwd:
                        auth = (user, pwd)
                        print(f"  {G}✅ Credentials loaded: {user}{X}")
                        break
            except Exception:
                pass
    if not auth:
        print(f"  {Y}⚠️  No credentials found — using anonymous mode{X}")
    
    print(f"\n  {B}🔄 Fetching flight data...{X}\n")
    
    # Try multiple attempts
    for attempt in range(3):
        print(f"  {B}[Attempt {attempt+1}/3]{X}", end=" ")
        
        r = bypass.get("https://opensky-network.org/api/states/all", auth=auth)
        
        if r and r.status_code == 200:
            data = r.json()
            states = data.get("states", [])
            
            # Statistics
            total = len(states)
            with_pos = sum(1 for s in states if s[5] and s[6])
            in_air = sum(1 for s in states if not s[8])
            countries = {}
            
            for s in states[:500]:
                if s[2]:
                    countries[s[2]] = countries.get(s[2], 0) + 1
            
            print(f"{G}✅ SUCCESS!{X}")
            
            # Enhanced output
            print(f"\n  {B}{'─'*56}{X}")
            print(f"  {B}📊 FLIGHT STATISTICS{X}")
            print(f"  {B}{'─'*56}{X}")
            print(f"  ✈️  Total aircraft     : {B}{total:,}{X}")
            print(f"  📍 With position      : {with_pos:,} ({with_pos/total*100:.1f}%)")
            print(f"  🔵 In air             : {in_air:,}")
            print(f"  🟢 On ground          : {total-in_air:,}")
            
            # Top countries
            print(f"\n  {B}🌍 TOP COUNTRIES{X}")
            top = sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]
            for i, (c, cnt) in enumerate(top, 1):
                print(f"    {i}. {c:<25} {cnt:>5,} aircraft")
            
            # Sample flights
            print(f"\n  {B}✈️  SAMPLE FLIGHTS (with position){X}")
            samples = [s for s in states if s[5] and s[6]][:5]
            for i, s in enumerate(samples, 1):
                cs = (s[1] or "Unknown").strip()
                alt = f"{s[7]:.0f}m" if s[7] else "N/A"
                spd = f"{s[9]*3.6:.0f}km/h" if s[9] else "N/A"
                print(f"    {i}. {cs:<12} | {s[2][:20]:<20} | Alt:{alt:<8} | Spd:{spd:<8}")
            
            print(f"\n  {G}{B}🎉 API accessible! Pipeline ready{X}")
            return
    
    print(f"\n  {R}{B}❌ Failed after 3 attempts{X}")
    print(f"\n  {B}💡 Solutions:{X}")
    print(f"    1. Register free account: opensky-network.org/user/register")
    print(f"    2. Save credentials in credentials.json")
    print(f"    3. Use different VPN server")
    print(f"    4. Run again in 5 minutes")

if __name__ == "__main__":
    main()