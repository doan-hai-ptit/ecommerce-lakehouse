class DataTransformer:
    @staticmethod
    def detailed_display(products):
        if not products: return

        header = f"{'ID':<10} | {'Tên Sản Phẩm':<35} | {'Giá':<12} | {'Đã bán':<8} | {'Thương hiệu'}"
        print(header)
        print("-" * len(header))
        
        for item in products:
            p_id = item.get('id', 'N/A')
            name = (item.get('name', 'N/A')[:32] + '..') if len(item.get('name', '')) > 32 else item.get('name', 'N/A')
            price = f"{item.get('price', 0):,.0f}đ"
            
            # Xử lý lấy số lượng bán từ object lồng nhau
            sold_info = item.get('quantity_sold', {})
            if isinstance(sold_info, dict):
                sold = sold_info.get('value', 0)
            else:
                sold = item.get('all_time_quantity_sold', 0)
                
            brand = item.get('brand_name', 'No Brand')
            print(f"{p_id:<10} | {name:<35} | {price:<12} | {sold:<8} | {brand}")