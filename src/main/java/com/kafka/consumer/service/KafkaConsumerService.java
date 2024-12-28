package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.entity.Employee;
import com.kafka.consumer.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @Autowired
    private EmployeeRepository employeeRepository;

    @KafkaListener(topics = "employee_topic",groupId = "employee_group")
    public void consume(String message)
    {
        try {
            ObjectMapper objectMapper=new ObjectMapper();
            Employee employee=objectMapper.readValue(message, Employee.class);
            employeeRepository.save(employee);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }


}