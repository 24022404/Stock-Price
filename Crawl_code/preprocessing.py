import pandas as pd 
import os 
import datetime as dt
import time
import sys
import yfinance as yf

# Hàm tải dữ liệu từ Yahoo Finance
def load_data_yfinance(ticker):
    """
    Tải dữ liệu chứng khoán từ Yahoo Finance
    Returns: True (success), False (failed), hoặc None (critical error)
    """
    try:
        time_now = dt.datetime.now()
        file_to_save = './datack/stock_market_data-{}_{}.csv'.format(ticker, time_now.strftime("%Y-%m-%d"))
        
        # Tạo thư mục datack nếu chưa có
        os.makedirs('./datack', exist_ok=True)
        
        if os.path.exists(file_to_save):
            print(f'✓ Exists')
            return True
        
        # Tải dữ liệu từ Yahoo Finance
        stock = yf.Ticker(ticker)
        df = stock.history(period="max")  # Lấy toàn bộ lịch sử
        
        # Kiểm tra có dữ liệu không
        if df.empty:
            print(f'No data')
            return False
        
        # Chỉ lấy các cột cần thiết và reset index
        df = df[['Open', 'High', 'Low', 'Close']].reset_index()
        
        # Chuyển đổi Date column
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        # Sắp xếp theo ngày
        df = df.sort_values(by='Date', ascending=True)
        
        # Lưu file
        df.to_csv(file_to_save, index=False, encoding='utf-8')
        print(f'✓ Saved ({len(df)} days)')
        return True
        
    except Exception as e:
        error_msg = str(e)[:50]
        print(f'Error: {error_msg}')
        return False

def save_checkpoint(processed_tickers, checkpoint_file='./datack/checkpoint.txt'):
    """Lưu checkpoint để tiếp tục sau"""
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(processed_tickers))

def load_checkpoint(checkpoint_file='./datack/checkpoint.txt'):
    """Đọc checkpoint"""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f)
    return set()

if __name__ == "__main__":
    print("=" * 70)
    print("📈 STOCK MARKET DATA CRAWLER (Yahoo Finance)")
    print("=" * 70)
    
    # Đọc danh sách symbols
    symbol_file = r"C:\Users\ducmi\OneDrive\Desktop\KyThuatVaCongNgheDuLieuLonChoTTNT\Assignment\Stock-Price\Crawl_code\symbol.txt"
    
    try:
        with open(symbol_file, 'r', encoding='utf-8-sig') as fh:
            all_tickers = [line.strip() for line in fh if line.strip()]
    except FileNotFoundError:
        print(f"❌ Cannot find file: {symbol_file}")
        sys.exit(1)
    
    # Bỏ dòng header
    if all_tickers and all_tickers[0].upper() in ['SYMBOL', 'TICKER']:
        all_tickers = all_tickers[1:]
    
    # Lọc ticker hợp lệ
    valid_tickers = [t for t in all_tickers if t and all(c.isalnum() or c in ['-', '.'] for c in t)]
    
    # Load checkpoint
    processed = load_checkpoint()
    remaining_tickers = [t for t in valid_tickers if t not in processed]
    
    print(f"\n📊 Statistics:")
    print(f"   Total symbols in file: {len(all_tickers)}")
    print(f"   Valid symbols: {len(valid_tickers)}")
    print(f"   Already processed: {len(processed)}")
    print(f"   Remaining to process: {len(remaining_tickers)}")
    
    if not remaining_tickers:
        print("\n✅ All symbols already processed!")
        sys.exit(0)
    
    # Ước tính thời gian (với yfinance nhanh hơn nhiều)
    estimated_minutes = len(remaining_tickers) * 2 / 60  # ~2 giây/ticker
    
    print(f"\n⏱️  Time estimates:")
    print(f"   Estimated time: {estimated_minutes:.1f} minutes ({estimated_minutes/60:.1f} hours)")
    print(f"\n✅ Using Yahoo Finance - No rate limits!")
    
    # Hỏi user có muốn tiếp tục không
    print("\n" + "=" * 70)
    response = input("Continue? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Cancelled.")
        sys.exit(0)
    
    print("\n" + "=" * 70)
    print("Starting crawler... (Press Ctrl+C to stop safely)\n")
    
    # Thống kê
    success_count = 0
    failed_count = 0
    already_exists = 0
    
    # Xử lý từng symbol
    try:
        for i, ticker in enumerate(remaining_tickers, 1):
            print(f"[{i}/{len(remaining_tickers)}] {ticker:8s} ... ", end="")
            
            # Kiểm tra file đã tồn tại
            time_now = dt.datetime.now()
            file_to_save = './datack/stock_market_data-{}_{}.csv'.format(ticker, time_now.strftime("%Y-%m-%d"))
            
            if os.path.exists(file_to_save):
                print("✓ Exists")
                already_exists += 1
                processed.add(ticker)
            else:
                result = load_data_yfinance(ticker)
                
                if result is True:
                    success_count += 1
                    processed.add(ticker)
                else:
                    failed_count += 1
            
            # Lưu checkpoint mỗi 50 symbols (tăng từ 10 vì nhanh hơn)
            if i % 50 == 0:
                save_checkpoint(list(processed))
                print(f"\n💾 Checkpoint saved at {i}/{len(remaining_tickers)}")
            
            # Delay nhẹ để tránh spam (1-2s là đủ)
            if i < len(remaining_tickers):
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("⚠️  INTERRUPTED BY USER")
        print("=" * 70)
    
    finally:
        # Lưu checkpoint cuối cùng
        save_checkpoint(list(processed))
        
        # Tổng kết
        print("\n" + "=" * 70)
        print("📋 SUMMARY")
        print("=" * 70)
        print(f"✅ Successfully downloaded: {success_count}")
        print(f"ℹ️  Already existed: {already_exists}")
        print(f"❌ Failed: {failed_count}")
        print(f"📁 Total files: {success_count + already_exists}")
        print(f"📍 Checkpoint saved - can resume later")
        print("=" * 70)