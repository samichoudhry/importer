"""
Tests for JSON variant field type functionality.
"""

import json
import pytest
from pathlib import Path
from lxml import etree

from multi_format_parser.xpath_utils import xml_element_to_json, _clean_namespaces_from_dict
from multi_format_parser.config_models import ParserConfig, FieldType


class TestXMLToJSON:
    """Test XML to JSON conversion for variant fields."""
    
    def test_simple_element(self):
        """Test converting simple XML element to JSON."""
        xml = '<Item><ID>123</ID><Name>Test</Name></Item>'
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem)
        parsed = json.loads(result)
        
        assert 'Item' in parsed
        assert parsed['Item']['ID'] == '123'
        assert parsed['Item']['Name'] == 'Test'
    
    def test_nested_element(self):
        """Test converting nested XML structure to JSON."""
        xml = '''<ItemLine>
            <ItemCode>
                <POSCode>00072250030625</POSCode>
                <InventoryItemID>5603</InventoryItemID>
            </ItemCode>
            <Description>MRS. FRESHLEY MINI CHOC.</Description>
            <ActualSalesPrice>2.49</ActualSalesPrice>
        </ItemLine>'''
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem)
        parsed = json.loads(result)
        
        assert 'ItemLine' in parsed
        assert parsed['ItemLine']['ItemCode']['POSCode'] == '00072250030625'
        assert parsed['ItemLine']['Description'] == 'MRS. FRESHLEY MINI CHOC.'
        assert parsed['ItemLine']['ActualSalesPrice'] == '2.49'
    
    def test_element_with_attributes(self):
        """Test converting XML element with attributes to JSON."""
        xml = '<TransactionLine status="normal"><ItemID>123</ItemID></TransactionLine>'
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem)
        parsed = json.loads(result)
        
        assert 'TransactionLine' in parsed
        assert parsed['TransactionLine']['@status'] == 'normal'
        assert parsed['TransactionLine']['ItemID'] == '123'
    
    def test_multiple_elements_returns_array(self):
        """Test that multiple elements return a JSON array."""
        xml = '<Root><Item>A</Item><Item>B</Item></Root>'
        root = etree.fromstring(xml)
        items = root.findall('Item')
        
        result = xml_element_to_json(items)
        parsed = json.loads(result)
        
        assert isinstance(parsed, list)
        assert len(parsed) == 2
        assert parsed[0]['Item'] == 'A'
        assert parsed[1]['Item'] == 'B'
    
    def test_single_element_returns_object(self):
        """Test that single element returns object, not array."""
        xml = '<Root><Item>A</Item></Root>'
        root = etree.fromstring(xml)
        items = root.findall('Item')
        
        result = xml_element_to_json(items)
        parsed = json.loads(result)
        
        assert isinstance(parsed, dict)
        assert parsed['Item'] == 'A'
    
    def test_force_list_option(self):
        """Test force_list option returns array for single element."""
        xml = '<Item>A</Item>'
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem, force_list=True)
        parsed = json.loads(result)
        
        assert isinstance(parsed, list)
        assert len(parsed) == 1
    
    def test_none_input(self):
        """Test None input returns None."""
        result = xml_element_to_json(None)
        assert result is None
    
    def test_empty_list_input(self):
        """Test empty list input returns None."""
        result = xml_element_to_json([])
        assert result is None
    
    def test_namespace_cleanup(self):
        """Test that XML namespaces are removed from JSON."""
        xml = '''<nax:Item xmlns:nax="http://example.com/ns">
            <nax:ID>123</nax:ID>
        </nax:Item>'''
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem, clean_namespaces=True)
        parsed = json.loads(result)
        
        # Should not contain @xmlns attributes
        assert '@xmlns' not in str(result)
        assert '@xmlns:nax' not in str(result)
    
    def test_special_characters(self):
        """Test handling of special characters in XML content."""
        xml = '<Description>He said "hello" &amp; goodbye</Description>'
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem)
        parsed = json.loads(result)
        
        assert 'He said "hello"' in parsed['Description']
        assert '&' in parsed['Description']
    
    def test_unicode_content(self):
        """Test handling of Unicode/emoji content."""
        xml = '<Description>Café ☕ 中文</Description>'
        elem = etree.fromstring(xml)
        
        result = xml_element_to_json(elem)
        parsed = json.loads(result)
        
        assert 'Café' in parsed['Description']
        assert '☕' in parsed['Description']
        assert '中文' in parsed['Description']


class TestCleanNamespaces:
    """Test namespace cleaning utility."""
    
    def test_clean_simple_xmlns(self):
        """Test removing @xmlns from dict."""
        data = {
            'Item': {
                '@xmlns': 'http://example.com',
                'ID': '123'
            }
        }
        
        cleaned = _clean_namespaces_from_dict(data)
        
        assert '@xmlns' not in cleaned['Item']
        assert cleaned['Item']['ID'] == '123'
    
    def test_clean_xmlns_prefix(self):
        """Test removing @xmlns:prefix from dict."""
        data = {
            'Item': {
                '@xmlns:nax': 'http://example.com',
                'ID': '123'
            }
        }
        
        cleaned = _clean_namespaces_from_dict(data)
        
        assert '@xmlns:nax' not in cleaned['Item']
        assert cleaned['Item']['ID'] == '123'
    
    def test_clean_nested_namespaces(self):
        """Test removing namespaces from nested structures."""
        data = {
            'Root': {
                '@xmlns': 'http://example.com',
                'Child': {
                    '@xmlns:nax': 'http://example.com/nax',
                    'Value': 'test'
                }
            }
        }
        
        cleaned = _clean_namespaces_from_dict(data)
        
        assert '@xmlns' not in cleaned['Root']
        assert '@xmlns:nax' not in cleaned['Root']['Child']
        assert cleaned['Root']['Child']['Value'] == 'test'
    
    def test_preserve_other_attributes(self):
        """Test that non-namespace attributes are preserved."""
        data = {
            'Item': {
                '@xmlns': 'http://example.com',
                '@status': 'active',
                '@id': '123',
                'Value': 'test'
            }
        }
        
        cleaned = _clean_namespaces_from_dict(data)
        
        assert '@xmlns' not in cleaned['Item']
        assert cleaned['Item']['@status'] == 'active'
        assert cleaned['Item']['@id'] == '123'
        assert cleaned['Item']['Value'] == 'test'


class TestJSONFieldTypeConfig:
    """Test JSON field type in configuration."""
    
    def test_json_field_type_in_enum(self):
        """Test that JSON is a valid field type."""
        assert FieldType.JSON == "json"
        assert "json" in [ft.value for ft in FieldType]
    
    def test_json_field_in_config(self):
        """Test creating config with JSON field type."""
        config_dict = {
            "format_type": "xml",
            "records": [{
                "name": "Test",
                "select": "//Item",
                "fields": [
                    {"name": "ID", "path": "ID", "type": "string"},
                    {"name": "Details", "path": "Details", "type": "json"}
                ]
            }]
        }
        
        config = ParserConfig.from_dict(config_dict)
        
        assert config.records[0].fields[1].type == FieldType.JSON


class TestJSONFieldIntegration:
    """Integration tests for JSON fields in actual parsing."""
    
    def test_parse_xml_with_json_field(self, tmp_path):
        """Test parsing XML file with JSON field type."""
        # Create test XML
        xml_content = '''<?xml version="1.0"?>
<Root>
    <Transaction>
        <ID>1</ID>
        <TransactionLine status="normal">
            <ItemLine>
                <ItemCode>12345</ItemCode>
                <Description>Test Item</Description>
                <Price>9.99</Price>
            </ItemLine>
        </TransactionLine>
    </Transaction>
</Root>'''
        
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)
        
        # Create config with JSON field
        config = {
            "format_type": "xml",
            "records": [{
                "name": "Transactions",
                "select": "//Transaction",
                "fields": [
                    {"name": "TransactionID", "path": "ID", "type": "string"},
                    {"name": "LineDetails", "path": "TransactionLine", "type": "json"}
                ]
            }]
        }
        
        # Parse (this tests integration with actual parser)
        from multi_format_parser.parsers.xml_parser import parse_xml
        from multi_format_parser.models import ParsingStats
        
        stats = {"Transactions": 0}
        record_stats = {"Transactions": ParsingStats()}
        
        success, error = parse_xml(xml_file, config, None, stats, record_stats)
        
        assert success
        assert error is None
        assert record_stats["Transactions"].total_rows > 0
